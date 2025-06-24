// HVM3-Strict Core: parallel, polarized, LAM/APP & DUP/SUP only
#define _GNU_SOURCE

#include <assert.h>
#include <execinfo.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// #define SUMMARY
//#define DEBUG

#ifdef __APPLE__
// Store barrier
extern void dmb_ishst(void);
__asm__(
    ".global dmb_ishst\n"
    ".global _dmb_ishst\n"
    "_dmb_ishst:\n"
    "dmb_ishst:\n"
    "    dmb ishst\n"
    "    ret\n"
);
#endif // __APPLE__

// --- Constants ---
#define VAR 0x01
#define SUB 0x02
#define NUL 0x03
#define ERA 0x04
#define LAM 0x05
#define APP 0x06
#define SUP 0x07
#define DUP 0x08
#define REF 0x09
#define OPX 0x0A
#define OPY 0x0B
#define U48 0x0C
#define I48 0x0D
#define F48 0x0E
#define MAT 0x0F

// --- Operators ---
#define OP_ADD 0x00
#define OP_SUB 0x01
#define OP_MUL 0x02
#define OP_DIV 0x03
#define OP_MOD 0x04
#define OP_EQ  0x05
#define OP_NE  0x06
#define OP_LT  0x07
#define OP_GT  0x08
#define OP_LTE 0x09
#define OP_GTE 0x0A
#define OP_AND 0x0B
#define OP_OR  0x0C
#define OP_XOR 0x0D
#define OP_LSH 0x0E
#define OP_RSH 0x0F

// --- Rusty Types --- 
typedef uint8_t u8;
typedef uint32_t u32;
typedef uint64_t u64;

typedef _Atomic(u32) a32;
typedef _Atomic(u64) a64;

typedef unsigned __int128 u128 __attribute__((aligned(16)));

typedef int32_t i32;
typedef int64_t i64;

typedef float f32;
typedef double f64;

typedef size_t usize;
typedef ptrdiff_t isize;

// --- CORE TYPES ---
typedef u64 Term;
typedef u64 Loc;
typedef u8 Lab;
typedef u8 Tag;

typedef u128 Pair;

// [Loc:48 | Lab:8 | Tag:8]
#define TAG_BITS 8
#define LAB_BITS 8
#define LOC_BITS 48

#define TAG_MASK ((1ULL << TAG_BITS) - 1)   // 0xFF
#define LAB_MASK ((1ULL << LAB_BITS) - 1)   // 0xFF
#define LOC_MASK ((1ULL << LOC_BITS) - 1)   // 0xFFFFFFFFFFFF

// -- Type Conversion --
typedef union {
  u64 u;
  i64 i;
  f64 f;
} TypeConverter;

// --- Heap Config --- 
enum : u64 {
  HEAP_1GB = (1ULL << 27) * sizeof(u64),  // 128Mi * 8 bytes = 1GiB

  //////////////////////////
  // Choose a heap size here
  //////////////////////////
  HEAP_SIZ = HEAP_1GB * 8,

  // Cache line size
  CACH_SIZ = 64,
  CACH_U64 = CACH_SIZ / sizeof(u64),

  // --- Number of threads ---
  TPC = 12,

  #ifdef __APPLE__
  // PCores and ECores total, and in-use
  PCOR_TOT = 4,
  ECOR_TOT = TPC - PCOR_TOT,
  PCOR = TPC < PCOR_TOT ? TPC : PCOR_TOT,
  ECOR = TPC > PCOR_TOT ? (TPC - PCOR) : 0,
  #endif

  // Misc
  ZERO = 0,
  IDLE = 256,

  // Deferred bag terms per thread (2 bags: twice this len used)
  DFER_LEN = 256,
  // When deferred bag gets this big, a memory sync occurs
  DFER_SYN = 32,

  // Includes booty and deferred bags
  RBAG_QED = 8192 * TPC * sizeof(Pair), // Demonstrated to work

  // Used in other calculations below
  RBAG_SIZ = RBAG_QED,
};

enum : u32 {
  // Most of these must be 16-byte aligned (even numbers).

  // Final calculated RBAG index
  RBAG = ((HEAP_SIZ - RBAG_SIZ) / sizeof(Term)) & ~1ULL,

  // Calculate RBAG_LEN and NODE_LEN as terms per thread
  RBAG_LEN = (RBAG_SIZ / (TPC * sizeof(Term))) & ~1ULL,
  NODE_LEN = (HEAP_SIZ - RBAG_SIZ) / (TPC * sizeof(Term)),

  // Booty bag terms per thread (also starting offset of RBAG)
  BBAG_LEN = 96,

  // Starting offset of deferred bags
  DFER_INI = (RBAG_LEN - (DFER_LEN * 2)) & ~1ULL,

  // Max terms allowed in RBAG, taking into account booty and deferred bags
  RPUT_MAX = DFER_INI - BBAG_LEN
};

typedef struct Net {
  a64 idle;
} Net;

// Global book
typedef struct Def {
  char *name;
  Term *nodes;
  u64  nodes_len;
  Term *rbag;
  u64  rbag_len;
} Def;

typedef struct Book {
  Def *defs;
  u32 len;
  u32 cap;
} Book;

// Local Thread Memory
typedef struct TM {
  u32 tid;   // thread id
  Loc nput;  // next node allocation attempt index
  Loc rput;  // next rbag push index
  Loc bput;  // owned bbag push index

  u32 sid;   // tid from which bbag was stolen (may be our own)
  Loc spop;  // stolen bbag pop index + 2

  Loc dput[2]; // next deferred bag push indices
  
  bool buse; // can use booty bag
  bool bhld; // when we KNOW booty bag ctrl word is HELD
             // (in some cases it may be HELD but we don't know)

  u8   dpid; // deferred bag idx we are pushing into
  bool duse; // can use deferred bag
  bool dsyn; // deferred bag is sync'd

  u8   lvic; // last failed steal attempt victim

#ifdef __APPLE__
  u8   pvic; // last failed PCore victim
  u8   evic; // last failed ECore victim
#endif // __APPLE__

  u64 itrs;  // interaction count
} TM;

// static_assert(sizeof(TM) <= CACH_SIZ, "TM struct getting big");

// Booty bag control word values
enum : u64 {
  HELD = 1,    // held by owner, or "returned" by another thread
  DROPPED = 2, // dropped by owner. can be picked back up by owner or stolen
               // by another thread
  STOLEN = 3,  // stolen by another thread
};

typedef struct BB {
  a64 ctrl; // control word
} BB;

// Global heap, net, book
static u64 *BUFF = NULL;
static Net net;
static Book BOOK = {
  .defs = NULL,
  .len = 0,
  .cap = 0,
};

static TM *tms[TPC];
static BB bbs[TPC];
static pthread_t threads[TPC];

// Debugging
static char *tag_to_str(Tag tag);
static const char* term_str(char* buf, Term term);

// FFI functions
void dump_buff();
Tag term_tag(Term term);
Loc term_loc(Term term);

static u64 align(u64 align, u64 val) {
  return (val + align - 1) & ~(align - 1);
}

// TM/BB operations
void tm_reset(TM *tm) {
  tm->rput = 0;
  tm->nput = 0;
  tm->itrs = 0;
}

TM *tm_new(u64 tid) {
  TM *tm = aligned_alloc(CACH_SIZ, align(CACH_SIZ, sizeof(TM)));
  if (tm == NULL) {
    fprintf(stderr, "tm_new() memory allocation failed\n");
    exit(1);
  }
  tm_reset(tm);

  tm->tid = tid;

  // Booty bag
  tm->bput = 0;
  tm->bhld = true;
  tm->buse = false;

  // Stolen booty bag
  tm->sid = TPC;
  tm->spop = 0;

  // Deferred bag
  tm->dput[0] = 0;
  tm->dput[1] = 0;
  tm->dpid = 0;
  tm->duse = false;
  tm->dsyn = false;

  // Last failed steal attempt thread ids
  tm->lvic = TPC;
#ifdef __APPLE__
  tm->pvic = tid % PCOR;
  tm->evic = (tid % ECOR) + PCOR;
#endif // __APPLE__

  return tm;
}

static void alloc_static_data() {
  for (u64 t = 0; t < TPC; ++t) {
    tms[t] = tm_new(t);
  }
}

static void free_static_data() {
  for (u64 t = 0; t < TPC; ++t) {
    if (tms[t] != NULL) {
      free(tms[t]);
      tms[t] = NULL;
    }
  }
}

// --- Pair operations ---
static Pair pair_new(Term neg, Term pos) {
  return ((Pair)pos << 64) | neg;
}

static Term pair_pos(Pair pair) {
  return (pair >> 64) & 0xFFFFFFFFFFFFFFFF;
}

static Term pair_neg(Pair pair) {
  return pair & 0xFFFFFFFFFFFFFFFF;
}

static Pair take_pair(Loc loc) {
  return *(Pair*)&BUFF[loc];
}

static void set_pair(Loc loc, Pair pair) {
  *((Pair*)&BUFF[loc]) = pair;
}

// --- Term operations --- 

extern inline Term term_new(Tag tag, Lab lab, Loc loc) {
    return ((Term)loc << (LAB_BITS + TAG_BITS)) |
           ((Term)lab << TAG_BITS) |
           ((Term)tag & TAG_MASK);
}

extern inline Term term_with_loc(Term term, Loc loc) {
    return (term & ~((Term)LOC_MASK << (LAB_BITS + TAG_BITS))) | 
           (((Term)loc & LOC_MASK) << (LAB_BITS + TAG_BITS));
}

extern inline Loc term_loc(Term term) {
    return term >> (LAB_BITS + TAG_BITS);
}

extern inline Lab term_lab(Term term) {
    return (term >> TAG_BITS) & LAB_MASK;
}

extern inline Tag term_tag(Term term) {
    return term & TAG_MASK;
}

static bool term_has_loc(Term term) {
  Tag tag = term_tag(term);
  return !(tag == SUB || tag == NUL || tag == ERA || tag == REF || tag == U48);
}

static Term term_offset_loc(Term term, Loc offset) {
  if (!term_has_loc(term)) { return term; }
  Loc loc = term_loc(term) + offset;
  return term_with_loc(term, loc);
}

static Loc port(u32 n, Loc loc) { return n + loc - 1; }

// Memory operations
Term swap(Loc loc, Term term) {
  return atomic_exchange_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);
}

Term take(Loc loc) {
  return atomic_exchange_explicit((a64*)&BUFF[loc], ZERO, memory_order_relaxed);
}

Term get(Loc loc) {
  return atomic_load_explicit((a64*)&BUFF[loc], memory_order_relaxed);
}

void set(Loc loc, Term term) {
  atomic_store_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);
}


// --- Booty Bag operations ---
static Loc bbag_offset(u32 tid) {
  return tid * RBAG_LEN;
}

static Loc bbag_ini(u32 tid) {
  return RBAG + bbag_offset(tid);
}

static bool bbag_empty(TM *tm) {
  return tm->bput == 0;
}

static bool bbag_full(TM *tm) {
  return tm->bput == BBAG_LEN;
}

static bool bbag_compare_swap(u32 tid, u64 expect, u32 desire,
                              memory_order success_order) {
  return atomic_compare_exchange_strong_explicit(&bbs[tid].ctrl, &expect,
      desire, success_order, memory_order_relaxed);
}

static void bbag_set(u32 tid, u32 val, memory_order order) {
  atomic_store_explicit(&bbs[tid].ctrl, val, order);
}

static u32 bbag_get(u32 tid) {
  return atomic_load_explicit(&bbs[tid].ctrl, memory_order_relaxed);
}

// Drop a held, full booty bag (make it steal-able)
static void bbag_drop(TM *tm) {
  bbag_set(tm->tid, DROPPED, memory_order_release);
  tm->bhld = false;
}

// Attempt to recover a stolen and emptied booty bag
static bool bbag_recover(TM *tm) {
  bool held = bbag_get(tm->tid) == HELD;
  if (held) {
    tm->bhld = true;
    tm->bput = 0;
  }
  return held;
}

// Attempt to pick up our own dropped booty bag - this is treated as "stealing"
// from ourself - the only way to pop from our own bag
static bool bbag_pickup(TM *tm) {
  bool held = bbag_compare_swap(tm->tid, DROPPED, HELD, memory_order_relaxed);
  if (held) {
    tm->bhld = true;
    tm->spop = tm->bput; // will always be BBAG_LEN
    tm->sid = tm->tid;
  }
  return held;
}

// Attempt to steal another thread's dropped booty bag
static bool bbag_steal(TM *tm, u32 sid) {
  bool stole = bbag_compare_swap(sid, DROPPED, STOLEN, memory_order_acquire);
  if (stole) {
    tm->spop = BBAG_LEN;
    tm->sid = sid;
  }
  return stole;
}

// Return true if booty bag is stolen, and empty
static bool bbag_looted(TM *tm) {
  return (tm->sid < TPC) && (tm->spop == 0);
}

// Return a stolen, empty booty bag
static void bbag_return(TM *tm) {
  if (tm->sid != tm->tid) {
    // It was another thread's bag - reset state to HELD
    bbag_set(tm->sid, HELD, memory_order_relaxed);
  }
  // No longer stealing
  tm->sid = TPC;
}

// --- RBAG operations ---
static Loc rbag_ini(u32 tid) {
  return bbag_ini(tid) + BBAG_LEN;
}

static bool rbag_empty(TM *tm) {
  return tm->rput == 0;
}

static Loc rnod_ini(u32 tid) {
  return tid * NODE_LEN;
}

// --- Deferred Bag operations ---
static Loc dfer_offset(u32 idx) {
  return DFER_INI + DFER_LEN * idx;
}

static Loc dfer_ini(u32 tid, u32 idx) {
  return bbag_ini(tid) + dfer_offset(idx);
}

// Check if the sync'd deferred bag we might pop from is empty
__attribute__((unused))
static bool dfer_empty(TM *tm) {
  return tm->dput[1u-tm->dpid] == 0;
}  

// Check if there are any items in either deferred bag
static bool dfer_any(TM *tm) {
  return (tm->dput[0] > 0) || (tm->dput[1] > 0);
}  

// --- Allocator --- 
static Loc node_alloc(TM *tm, u32 cnt) {
  if (tm->nput + cnt >= NODE_LEN) {
    fprintf(stderr, "%u node space exhausted, nput: %lu, cnt: %u, LEN: %u\n",
            tm->tid, tm->nput, cnt, NODE_LEN);
    exit(1);
  }

  Loc loc = rnod_ini(tm->tid) + tm->nput;
  tm->nput += cnt;
  return loc;
}

static Loc get_push_offset(TM *tm, bool dfer) {
  // Push to deferred bag
  if (dfer && tm->duse) {
    u32 dput = tm->dput[tm->dpid];
    tm->dput[tm->dpid] += 2;
    return dfer_offset(tm->dpid) + dput;
  }

  // Only push to booty bag if we aren't stealing
  if (tm->buse && tm->bhld && !bbag_full(tm) && (tm->sid == TPC)) {
    u32 bput = tm->bput;
    tm->bput += 2;
    return bput;
  }

  // Push to RBAG
  u32 rput = tm->rput;
  tm->rput += 2;
  return BBAG_LEN + rput;
}

static void redex_push(TM *tm, Term neg, Term pos, bool dfer) {
  Loc off = get_push_offset(tm, dfer);
  Loc loc = bbag_ini(tm->tid) + off;

  set_pair(loc, pair_new(neg, pos));

  if (off < BBAG_LEN) {
    // We pushed to booty bag
    if (bbag_full(tm)) {
      // We're holding a full, non-stolen booty bag - if there are terms in
      // other bags available to pop immediately, drop it
      if (!rbag_empty(tm) || dfer_any(tm)) {
        bbag_drop(tm);
      }
    }
  }

  #ifdef DEBUG
  if (off > BBAG_LEN) {
    // Pushed to RBAG
    if (tm->rput >= RPUT_MAX-1) {
      fprintf(stderr, "%u rbag space exhausted\n", tm->tid);
      exit(1);
    }
  }
  #endif
}

static Loc dfer_pop_loc(TM *tm) {
  if (tm->dsyn) {
    // Deferred bag is sync'd so we can pop
    u32 dpid = 1u - tm->dpid;
    if (tm->dput[dpid] > 0) {
      tm->dput[dpid] -= 2;
      return dfer_ini(tm->tid, dpid) + tm->dput[dpid];
    } else {
      tm->dsyn = false;
    }
  }
  return 0;
}

static void dfer_sync(TM *tm) {
#ifdef __APPLE__
  dmb_ishst();
#endif // __APPLE__
  tm->dpid = 1u - tm->dpid;
  tm->dsyn = true;
}

static Loc redex_pop_loc(TM* tm) {
  // Check deferred bags first
  if (tm->duse) {
    Loc loc = dfer_pop_loc(tm);
    if (loc > 0) return loc;

    // Flip & sync deferred bag if size has reached threshold
    if (tm->dput[tm->dpid] > DFER_SYN) {
      dfer_sync(tm);
      Loc loc = dfer_pop_loc(tm);
      if (loc > 0) return loc;
    }
  }
    
  if ((tm->sid < TPC) && (tm->spop > 0)) {
    // Pop from stolen booty bag - it may be our HELD bag
    tm->spop -= 2;

    // If we are stealing from our own bag, adjust push index as well
    if (tm->sid == tm->tid) {
      tm->bput -= 2;
    }
    return bbag_ini(tm->sid) + tm->spop;
  } else if (tm->rput > 0) {
    // Pop from RBAG
    tm->rput -= 2;
    return rbag_ini(tm->tid) + tm->rput;
  } else if (tm->duse) {
    // Finally, try a flip & sync deferred bag with *any* elems
    if (tm->dput[tm->dpid] > 0) {
      dfer_sync(tm);
      return dfer_pop_loc(tm);
    }
  }
  // Steal from someone else
  return 0;
}

// FFI functions
void hvm_init() {
  if (BUFF == NULL) {
    BUFF = aligned_alloc(CACH_SIZ, HEAP_SIZ);
    if (BUFF == NULL) {
      fprintf(stderr, "Heap memory allocation failed\n");
      exit(1);
    }
  }
  memset(BUFF, 0, HEAP_SIZ);

  alloc_static_data();

  #ifdef SUMMARY
  fprintf(stderr, "HEAP_SIZ = %" PRIu64 "\n", HEAP_SIZ);
  fprintf(stderr, "RBAG_SIZ = %" PRIu64 "\n", RBAG_SIZ);
  fprintf(stderr, "RBAG     = %u\n", RBAG);
  fprintf(stderr, "RBAG_LEN = %u\n", RBAG_LEN);
  fprintf(stderr, "NODE_LEN = %u\n", NODE_LEN);
  fprintf(stderr, "BBAG_LEN = %u\n", BBAG_LEN);
  fprintf(stderr, "DFER_INI = %u\n", DFER_INI);
  fprintf(stderr, "DFER_LEN = %" PRIu64 "\n", DFER_LEN);
  #endif
}

void hvm_free() {
  if (BUFF != NULL) {
    free(BUFF);
    BUFF = NULL;
  }
  free_static_data();
}

void handle_failure() {
}

Loc ffi_alloc_node(u64 arity) {
  TM *tm = tms[0];
  Loc loc = tm->nput;
  tm->nput += arity;
  return loc;
}

void ffi_rbag_push(Term neg, Term pos) {
  redex_push(tms[0], neg, pos, false);
}

u64 inc_itr() {
  u64 itrs = 0;
  for (u32 i = 0; i < TPC; i++) {
    itrs += tms[i]->itrs;
  }
  return itrs;
} 

Loc ffi_rbag_ini() {
  // TODO
  return RBAG;
}

Loc ffi_rbag_end() {
  // TODO
  return RBAG;
}

Loc ffi_rnod_end() {
  u64 nods = 0;
  for (u32 i = 0; i < TPC; i++) {
    nods += tms[i]->nput;
  }
  return nods;
}

// Moves the global buffer and redex bag into a new def and resets
// the global buffer and redex bag.
void def_new(char *name) {
  if (BOOK.len == BOOK.cap) {
    if (BOOK.cap == 0) {
      BOOK.cap = 32;
    } else {
      BOOK.cap *= 2;
    }
    BOOK.defs = realloc(BOOK.defs, sizeof(Def) * BOOK.cap);
  }

  TM *tm = tms[0];

  Loc rnod_cnt = tm->nput;
  Loc rbag_cnt = tm->rput;

  u64 rbag_siz = sizeof(Term) * rbag_cnt;
  u64 rnod_siz = sizeof(Term) * rnod_cnt;

  Def def = {
      .name = name,
      .nodes = aligned_alloc(CACH_SIZ, align(CACH_SIZ, rnod_siz)),
      .nodes_len = rnod_cnt,
      .rbag = aligned_alloc(CACH_SIZ, align(CACH_SIZ, rbag_siz)),
      .rbag_len = rbag_cnt,
  };

  if ((def.nodes == NULL) || (def.rbag == NULL)) {
    fprintf(stderr, "def_new() memory allocation failed\n");
    exit(1);
  }

  Term *rnod = BUFF;
  Term *rbag = &BUFF[rbag_ini(0)];

  memcpy(def.nodes, rnod, rnod_siz);
  memcpy(def.rbag, rbag, rbag_siz);

  #if 0
  printf("NEW DEF '%s':\n", def.name);
  dump_buff();
  printf("\n");
  #endif

  memset(rnod, 0, rnod_siz);
  memset(rbag, 0, rbag_siz);

  BOOK.defs[BOOK.len] = def;
  BOOK.len++;

  tm_reset(tm);
}

char *def_name(Loc def_idx) { return BOOK.defs[def_idx].name; }

// Expands a ref's data into a linear block of nodes with its nodes' locs
// offset by the index where in the BUFF it was expanded.
//
// Returns the ref's root, the first node in its data.
static Term expand_ref(TM *tm, Loc def_idx) {
  // Get definition data
  const Def* def = &BOOK.defs[def_idx];
  const u32 nodes_len = def->nodes_len;
  const Term *nodes = def->nodes;
  const Term *rbag = def->rbag;
  const u32 rbag_len = def->rbag_len;

  // offset calculation must occur before node_alloc() call
  // TODO: i think we can use the return value of node_alloc() here
  Loc offset = (tm->tid * NODE_LEN) + tm->nput - 1;
  node_alloc(tm, nodes_len - 1);

  Term root = term_offset_loc(nodes[0], offset);

  // No redexes reference these nodes yet; safe to add without atomics
  for (u32 n = 1; n < nodes_len; n++) {
    Loc loc = offset + n;
    Term term = term_offset_loc(nodes[n], offset);
    BUFF[loc] = term;
  }

  for (u32 i = 0; i < rbag_len; i += 2) {
    Term neg = term_offset_loc(rbag[i], offset);
    Term pos = term_offset_loc(rbag[i+1], offset);
    redex_push(tm, neg, pos, false);
  }
  return root;
}

static void boot(Loc def_idx) {
  TM *tm = tms[0];
  if (tm->nput > 0 || tm->rput > 0) {
    fprintf(stderr, "booting on non-empty state\n");
    exit(1);
  }
  node_alloc(tm, 1);
  set(0, expand_ref(tm, def_idx));
}

// Atomic Linker
static inline void move(TM *tm, Loc neg_loc, u64 pos);

static inline void link_terms(TM *tm, Term neg, Term pos) {
  if (term_tag(pos) == VAR) {
    Term far = swap(term_loc(pos), neg);
    if (term_tag(far) != SUB) {
      move(tm, term_loc(pos), far);
    }
  } else {
    redex_push(tm, neg, pos, false);
  }
}

static inline void move(TM *tm, Loc neg_loc, Term pos) {
  Term neg = swap(neg_loc, pos);
  if (term_tag(neg) != SUB) {
    // No need to take() since we already swapped
    link_terms(tm, neg, pos);
  }
}

// --- Interactions ---
static void interact_appref(TM *tm, Term neg, Loc pos_loc) {
  Term lam = expand_ref(tm, pos_loc);
  #ifdef DEBUG
  if (term_tag(lam) != LAM) {
    // Assumption broken. May not matter, but I want to know.
    fprintf(stderr, "APPREF root node is not a LAM, %s\n",
            tag_to_str(term_tag(lam)));
    exit(1);
  }
  #endif
  // Force push to deferred bag
  redex_push(tm, neg, lam, true);
}

static void interact_applam(TM *tm, Loc a_loc, Loc b_loc) {
  Term arg = take(port(1, a_loc));
  Loc var = port(1, b_loc);
  Loc ret = port(2, a_loc);
  Term bod = take(port(2, b_loc));

  move(tm, var, arg);
  move(tm, ret, bod);
}

static void interact_appsup(TM *tm, Loc a_loc, Loc b_loc) {
  Loc nloc = node_alloc(tm, 8);
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = nloc;
  Loc dp2 = nloc + 2;
  Loc cn1 = nloc + 4;
  Loc cn2 = nloc + 6;
  set(port(1, dp1), term_new(SUB, 0, 0));
  set(port(2, dp1), term_new(SUB, 0, 0));
  set(port(1, dp2), term_new(VAR, 0, port(2, cn1)));
  set(port(2, dp2), term_new(VAR, 0, port(2, cn2)));
  set(port(1, cn1), term_new(VAR, 0, port(1, dp1)));
  set(port(2, cn1), term_new(SUB, 0, 0));
  set(port(1, cn2), term_new(VAR, 0, port(2, dp1)));
  set(port(2, cn2), term_new(SUB, 0, 0));
  link_terms(tm, term_new(DUP, 0, dp1), arg);
  move(tm, ret, term_new(SUP, 0, dp2));
  link_terms(tm, term_new(APP, 0, cn1), tm1);
  link_terms(tm, term_new(APP, 0, cn2), tm2);
}

static void interact_appnul(TM *tm, Loc a_loc) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(ERA, 0, 0), arg);
  move(tm, ret, term_new(NUL, 0, 0));
}

static void interact_appu32(TM *tm, Loc a_loc, u32 num) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(U48, 0, num), arg);
  move(tm, ret, term_new(U48, 0, num));
}

static void interact_opxnul(TM *tm, Loc a_loc) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(ERA, 0, 0), arg);
  move(tm, ret, term_new(NUL, 0, 0));
}

static void interact_opxnum(TM *tm, Loc loc, Lab op, u64 num, Tag num_type) {
  Term arg = swap(port(1, loc), term_new(num_type, 0, num));
  link_terms(tm, term_new(OPY, op, loc), arg);
}

static void interact_opxsup(TM *tm, Loc a_loc, Lab op, Loc b_loc) {
  Loc nloc = node_alloc(tm, 8);
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = nloc;
  Loc dp2 = nloc + 2;
  Loc cn1 = nloc + 4;
  Loc cn2 = nloc + 6;
  set(port(1, dp1), term_new(SUB, 0, 0));
  set(port(2, dp1), term_new(SUB, 0, 0));
  set(port(1, dp2), term_new(VAR, 0, port(2, cn1)));
  set(port(2, dp2), term_new(VAR, 0, port(2, cn2)));
  set(port(1, cn1), term_new(VAR, 0, port(1, dp1)));
  set(port(2, cn1), term_new(SUB, 0, 0));
  set(port(1, cn2), term_new(VAR, 0, port(2, dp1)));
  set(port(2, cn2), term_new(SUB, 0, 0));
  link_terms(tm, term_new(DUP, 0, dp1), arg);
  move(tm, ret, term_new(SUP, 0, dp2));
  link_terms(tm, term_new(OPX, op, cn1), tm1);
  link_terms(tm, term_new(OPX, op, cn2), tm2);
}

static void interact_opynul(TM *tm, Loc a_loc) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(ERA, 0, 0), arg);
  move(tm, ret, term_new(NUL, 0, 0));
}

// Safer Utilities
u64 u64_to_u64(u64 u) { return u; }

i64 u64_to_i64(u64 u) {
  TypeConverter converter;
  converter.u = u;
  return converter.i;
}

f64 u64_to_f64(u64 u) {
  TypeConverter converter;
  converter.u = u;
  return converter.f;
}

u64 i64_to_u64(i32 i) {
  TypeConverter converter;
  converter.i = i;
  return converter.u;
}

u32 f64_to_u64(f64 f) {
  TypeConverter converter;
  converter.f = f;
  return converter.u;
}

static void interact_opynum(TM *tm, Loc a_loc, Lab op, u64 y, Tag y_type) {
  u64 x = term_loc(take(port(1, a_loc)));
  Loc ret = port(2, a_loc);
  u64 res;
  
  static void *dispatch[] = {
    [0xC0] = &&u_add, [0xC1] = &&u_sub, [0xC2] = &&u_mul, [0xC3] = &&u_div,
    [0xC4] = &&u_mod, [0xC5] = &&u_eq,  [0xC6] = &&u_ne,  [0xC7] = &&u_lt,
    [0xC8] = &&u_gt,  [0xC9] = &&u_lte, [0xCA] = &&u_gte, [0xCB] = &&u_and,
    [0xCC] = &&u_or,  [0xCD] = &&u_xor, [0xCE] = &&u_lsh, [0xCF] = &&u_rsh,
    
    [0xD0] = &&i_add, [0xD1] = &&i_sub, [0xD2] = &&i_mul, [0xD3] = &&i_div,
    [0xD4] = &&i_mod, [0xD5] = &&i_eq,  [0xD6] = &&i_ne,  [0xD7] = &&i_lt,
    [0xD8] = &&i_gt,  [0xD9] = &&i_lte, [0xDA] = &&i_gte, [0xDB] = &&i_and,
    [0xDC] = &&i_or,  [0xDD] = &&i_xor, [0xDE] = &&i_lsh, [0xDF] = &&i_rsh,
    
    [0xE0] = &&f_add, [0xE1] = &&f_sub, [0xE2] = &&f_mul, [0xE3] = &&f_div,
    [0xE4] = &&inv,   [0xE5] = &&f_eq,  [0xE6] = &&f_ne,  [0xE7] = &&f_lt,
    [0xE8] = &&f_gt,  [0xE9] = &&f_lte, [0xEA] = &&f_gte, [0xEB] = &&inv,
    [0xEC] = &&inv,   [0xED] = &&inv,   [0xEE] = &&inv,   [0xEF] = &&inv
  };
  
  goto *dispatch[(y_type << 4) | op];
  
  u_add: res = x + y; goto done;
  u_sub: res = x - y; goto done;
  u_mul: res = x * y; goto done;
  u_div: res = x / y; goto done;
  u_mod: res = x % y; goto done;
  u_eq:  res = x == y; goto done;
  u_ne:  res = x != y; goto done;
  u_lt:  res = x < y; goto done;
  u_gt:  res = x > y; goto done;
  u_lte: res = x <= y; goto done;
  u_gte: res = x >= y; goto done;
  u_and: res = x & y; goto done;
  u_or:  res = x | y; goto done;
  u_xor: res = x ^ y; goto done;
  u_lsh: res = x << y; goto done;
  u_rsh: res = x >> y; goto done;
  
  i_add: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a + b); goto done; }
  i_sub: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a - b); goto done; }
  i_mul: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a * b); goto done; }
  i_div: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a / b); goto done; }
  i_mod: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a % b); goto done; }
  i_eq:  { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a == b); goto done; }
  i_ne:  { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a != b); goto done; }
  i_lt:  { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a < b); goto done; }
  i_gt:  { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a > b); goto done; }
  i_lte: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a <= b); goto done; }
  i_gte: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a >= b); goto done; }
  i_and: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a & b); goto done; }
  i_or:  { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a | b); goto done; }
  i_xor: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a ^ b); goto done; }
  i_lsh: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a << b); goto done; }
  i_rsh: { i32 a = (i32)x, b = u64_to_i64(y); res = i64_to_u64(a >> b); goto done; }
  
  f_add: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a + b); goto done; }
  f_sub: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a - b); goto done; }
  f_mul: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a * b); goto done; }
  f_div: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a / b); goto done; }
  f_eq:  { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a == b); goto done; }
  f_ne:  { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a != b); goto done; }
  f_lt:  { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a < b); goto done; }
  f_gt:  { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a > b); goto done; }
  f_lte: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a <= b); goto done; }
  f_gte: { f64 a = u64_to_f64(x), b = u64_to_f64(y); res = f64_to_u64(a >= b); goto done; }
  
  inv: res = 0; // fallback
  
done:
  move(tm, ret, term_new(y_type, 0, res));
}

static void interact_opysup(TM *tm, Loc a_loc, Loc b_loc) {
  Loc nloc = node_alloc(tm, 8);
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = nloc;
  Loc dp2 = nloc + 2;
  Loc cn1 = nloc + 4;
  Loc cn2 = nloc + 6;
  set(port(1, dp1), term_new(SUB, 0, 0));
  set(port(2, dp1), term_new(SUB, 0, 0));
  set(port(1, dp2), term_new(VAR, 0, port(2, cn1)));
  set(port(2, dp2), term_new(VAR, 0, port(2, cn2)));
  set(port(1, cn1), term_new(VAR, 0, port(1, dp1)));
  set(port(2, cn1), term_new(SUB, 0, 0));
  set(port(1, cn2), term_new(VAR, 0, port(2, dp1)));
  set(port(2, cn2), term_new(SUB, 0, 0));
  link_terms(tm, term_new(DUP, 0, dp1), arg);
  move(tm, ret, term_new(SUP, 0, dp2));
  link_terms(tm, term_new(OPY, 0, cn1), tm1);
  link_terms(tm, term_new(OPY, 0, cn2), tm2);
}

static void interact_dupsup(TM *tm, Loc a_loc, Loc b_loc) {
  Loc dp1 = port(1, a_loc);
  Loc dp2 = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  move(tm, dp1, tm1);
  move(tm, dp2, tm2);
}

static void interact_duplam(TM *tm, Loc a_loc, Loc b_loc) {
  Loc nloc = node_alloc(tm, 8);
  Loc dp1 = port(1, a_loc);
  Loc dp2 = port(2, a_loc);
  Loc var = port(1, b_loc);
  // TODO(enricozb): why is this the only take?
  Term bod = take(port(2, b_loc));
  Loc co1 = nloc;
  Loc co2 = nloc + 2;
  Loc du1 = nloc + 4;
  Loc du2 = nloc + 6;
  set(port(1, co1), term_new(SUB, 0, 0));
  set(port(2, co1), term_new(VAR, 0, port(1, du2)));
  set(port(1, co2), term_new(SUB, 0, 0));
  set(port(2, co2), term_new(VAR, 0, port(2, du2)));
  set(port(1, du1), term_new(VAR, 0, port(1, co1)));
  set(port(2, du1), term_new(VAR, 0, port(1, co2)));
  set(port(1, du2), term_new(SUB, 0, 0));
  set(port(2, du2), term_new(SUB, 0, 0));
  move(tm, dp1, term_new(LAM, 0, co1));
  move(tm, dp2, term_new(LAM, 0, co2));
  move(tm, var, term_new(SUP, 0, du1));
  link_terms(tm, term_new(DUP, 0, du2), bod);
}

static void interact_dupnul(TM *tm, Loc a_loc) {
  Loc dp1 = port(1, a_loc);
  Loc dp2 = port(2, a_loc);
  move(tm, dp1, term_new(NUL, 0, a_loc));
  move(tm, dp2, term_new(NUL, 0, a_loc));
}

static void interact_dupnum(TM *tm, Loc a_loc, u32 n, Tag n_type) {
  Loc dp1 = port(1, a_loc);
  Loc dp2 = port(2, a_loc);
  move(tm, dp1, term_new(n_type, 0, n));
  move(tm, dp2, term_new(n_type, 0, n));
}

static void interact_dupref(TM *tm, Loc a_loc, Loc b_loc) {
  move(tm, port(1, a_loc), term_new(REF, 0, b_loc));
  move(tm, port(2, a_loc), term_new(REF, 0, b_loc));
}

static void interact_matnul(TM *tm, Loc a_loc, Lab mat_len) {
  move(tm, port(1, a_loc), term_new(NUL, 0, 0));
  for (u32 i = 0; i < mat_len; i++) {
    link_terms(tm, term_new(ERA, 0, 0), take(port(i + 2, a_loc)));
  }
}

static void interact_matnum(TM *tm, Loc mat_loc, Lab mat_len, u32 n, Tag n_type) {
  if (n_type != U48) {
    fprintf(stderr, "match with non-U48\n");
    exit(1);
  }

  u32 i_arm = (n < mat_len - 1) ? n : (mat_len - 1);
  for (u32 i = 0; i < mat_len; i++) {
    if (i != i_arm) {
      link_terms(tm, term_new(ERA, 0, 0), take(port(2 + i, mat_loc)));
    }
  }

  Loc ret = port(1, mat_loc);
  Term arm = take(port(2 + i_arm, mat_loc));
  if (i_arm < mat_len - 1) {
    move(tm, ret, arm);
  } else {
    Loc app = node_alloc(tm, 2);
    set(app + 0, term_new(U48, 0, n - (mat_len - 1)));
    set(app + 1, term_new(SUB, 0, 0));
    move(tm, ret, term_new(VAR, 0, port(2, app)));

    link_terms(tm, term_new(APP, 0, app), arm);
  }
}

// NOTE: not tested
static void interact_matsup(TM *tm, Loc mat_loc, Lab mat_len, Loc sup_loc) {
  Loc ma0 = node_alloc(tm, mat_len + 1);
  Loc ma1 = node_alloc(tm, mat_len + 1);
  Loc sup = node_alloc(tm, 2);

  set(port(1, sup), term_new(VAR, 0, port(1, ma1)));
  set(port(2, sup), term_new(VAR, 0, port(1, ma0)));
  set(port(1, ma0), term_new(SUB, 0, 0));
  set(port(1, ma1), term_new(SUB, 0, 0));

  for (u32 i = 0; i < mat_len; i++) {
    Loc dui = node_alloc(tm, 2);
    set(port(1, dui), term_new(SUB, 0, 0));
    set(port(2, dui), term_new(SUB, 0, 0));
    set(port(2 + i, ma0), term_new(VAR, 0, port(2, dui)));
    set(port(2 + i, ma1), term_new(VAR, 0, port(1, dui)));

    link_terms(tm, term_new(DUP, 0, dui), take(port(2 + i, mat_loc)));
  }

  move(tm, port(1, mat_loc), term_new(SUP, 0, sup));
  link_terms(tm, term_new(MAT, mat_len, ma0), take(port(2, sup_loc)));
  link_terms(tm, term_new(MAT, mat_len, ma1), take(port(1, sup_loc)));
}

static void interact_eralam(TM *tm, Loc b_loc) {
  Loc var = port(1, b_loc);
  Term bod = take(port(2, b_loc));
  move(tm, var, term_new(NUL, 0, 0));
  link_terms(tm, term_new(ERA, 0, 0), bod);
}

static void interact_erasup(TM *tm, Loc b_loc) {
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  link_terms(tm, term_new(ERA, 0, 0), tm1);
  link_terms(tm, term_new(ERA, 0, 0), tm2);
}

static bool interact(TM *tm, Term neg, Term pos) {
  Tag neg_tag = term_tag(neg);
  Tag pos_tag = term_tag(pos);
  Loc neg_loc = term_loc(neg);
  Loc pos_loc = term_loc(pos);

  switch (neg_tag) {
  case APP:
    switch (pos_tag) {
    case LAM:
      interact_applam(tm, neg_loc, pos_loc);
      break;
    case NUL:
      interact_appnul(tm, neg_loc);
      break;
    case U48:
      interact_appu32(tm, neg_loc, pos_loc);
      break;
    case REF:
      interact_appref(tm, neg, pos_loc);
      break;
    case SUP:
      interact_appsup(tm, neg_loc, pos_loc);
      break;
    default:
      break;
    }
    break;
  case OPX:
    switch (pos_tag) {
    case NUL:
      interact_opxnul(tm, neg_loc);
      break;
    case U48:
    case I48:
    case F48:
      interact_opxnum(tm, neg_loc, term_lab(neg), pos_loc, pos_tag);
      break;
    case REF:
      link_terms(tm, neg, expand_ref(tm, pos_loc));
      break;
    case SUP:
      interact_opxsup(tm, neg_loc, term_lab(neg), pos_loc);
      break;
    case LAM:
    default:
      break;
    }
    break;
  case OPY:
    switch (pos_tag) {
    case NUL:
      interact_opynul(tm, neg_loc);
      break;
    case U48:
    case I48:
    case F48:
      interact_opynum(tm, neg_loc, term_lab(neg), pos_loc, pos_tag);
      break;
    case REF:
      link_terms(tm, neg, expand_ref(tm, pos_loc));
      break;
    case SUP:
      interact_opysup(tm, neg_loc, pos_loc);
      break;
    case LAM:
    default:
      break;
    }
    break;
  case DUP:
    switch (pos_tag) {
    case LAM:
      interact_duplam(tm, neg_loc, pos_loc);
      break;
    case NUL:
      interact_dupnul(tm, neg_loc);
      break;
    case U48:
    case I48:
    case F48:
      interact_dupnum(tm, neg_loc, pos_loc, pos_tag);
      break;
    case REF:
      interact_dupref(tm, neg_loc, pos_loc);
      break;
    case SUP:
      interact_dupsup(tm, neg_loc, pos_loc);
      break;
    default:
      break;
    }
    break;
  case MAT:
    switch (pos_tag) {
    case NUL:
      interact_matnul(tm, neg_loc, term_lab(neg));
      break;
    case U48:
    case I48:
    case F48:
      interact_matnum(tm, neg_loc, term_lab(neg), pos_loc, pos_tag);
      break;
    case REF:
      link_terms(tm, neg, expand_ref(tm, pos_loc));
      break;
    case SUP:
      interact_matsup(tm, neg_loc, term_lab(neg), pos_loc);
      break;
    case LAM:
    default:
      break;
    }
    break;
  case ERA:
    switch (pos_tag) {
    case LAM:
      interact_eralam(tm, pos_loc);
      break;
    case SUP:
      interact_erasup(tm, pos_loc);
      break;
    case NUL:
    case U48:
    case REF:
    default:
      break;
    }
    break;
  default:
    break;
  }
  tm->itrs += 1;
  return true;
}

static bool sequential_step(TM* tm) {
  Loc loc = redex_pop_loc(tm);
  if (loc == 0) {
    return false;
  }
  Pair pair = take_pair(loc);
  interact(tm, pair_neg(pair), pair_pos(pair));
  return true;
}
 
static bool can_idle(TM *tm) {
  return rbag_empty(tm) && bbag_empty(tm) && !dfer_any(tm);
}
 
static bool set_idle(bool was_busy) {
  if (was_busy) {
    atomic_fetch_add_explicit(&net.idle, 1, memory_order_relaxed);
  }
  return false;
}

static bool set_busy(bool was_busy) {
  if (!was_busy) {
    atomic_fetch_sub_explicit(&net.idle, 1, memory_order_relaxed);
  }
  return true;
}

static u32 get_victim(TM* tm) {
#ifdef __APPLE__
  // PCore bias: if we didn't fail last attempt, or we failed on ECore, try PCore
  if ((tm->lvic == TPC) || (tm->lvic >= PCOR)) {
    tm->pvic = (tm->pvic - 1) % PCOR;
    return tm->pvic;
  } else {
    tm->evic = (tm->evic - 1) % ECOR + PCOR;
    return tm->evic;
  }
#else
  return (tm->tid - 1) % TPC;
#endif
}

static bool try_steal(TM *tm) {
  if (!tm->buse) return false;

  if (tm->bput > 0) {
    // Either our booty bag has something in it, or it had something in it
    // and we dropped it.
    if (tm->bhld) {
      // We're holding it, so we can "steal" (from) it without atomics
      tm->spop = tm->bput;
      tm->sid = tm->tid;
      return true;
    } else {
      // We dropped it - try to pick it up
      if (bbag_pickup(tm)) {
        return true;
      }
    }
  }
  // Our booty bag is either empty, or another thread stole it - try to steal
  // another thread's full bag
  u32 vic = get_victim(tm);
  if (bbag_steal(tm, vic)) {
    tm->lvic = TPC;
    return true;
  }
  tm->lvic = vic;
  return false;
}

static bool timeout(u64 tick) {
  if (tick % IDLE == 0) {
    u32 idle = atomic_load_explicit(&net.idle, memory_order_relaxed);
    if (idle == TPC) {
      return true;
    }
  }
  return false;
}

// --- Core binding (APPLE) ---
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/thread_policy.h>

void set_affinity(int tid) {
  thread_affinity_policy_data_t policy;
  policy.affinity_tag = tid; // 0-3 for P-cores, 4-9 for E-cores
  thread_policy_set(mach_thread_self(), THREAD_AFFINITY_POLICY,
                    (thread_policy_t)&policy, THREAD_AFFINITY_POLICY_COUNT);
}

void bind_pcore(int tid) {
    // Set high QoS
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    set_affinity(tid);
}

void bind_ecore(int tid) {
    // Set lower QoS
    pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0);
    set_affinity(tid);
}

static void bind_core(int tid) {
  if (tid < PCOR) {
    bind_pcore(tid);
  } else {
    bind_ecore(tid);
  }
}
#endif // __APPLE__

// --- Core binding (LINUX) ---
#ifdef __linux__
void bind_core(int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
#endif // __linux__ 

// --- Core thread function ---
static void* thread_func(void* arg) {
  int thread_id = (u64)arg;

  bind_core(thread_id);
  
  TM *tm = tms[thread_id];

  // Wait until after injection to turn these on
  tm->buse = true;
  tm->duse = true;

  __attribute__((unused))
  u64  fst_steal = 0;
  u64  tick = 0;
  bool busy = tm->tid == 0;
  while (true) {
    tick += 1;

    // If we know we're not holding our own booty bag, it may have been stolen
    // and returned - try to recover it
    if (!tm->bhld) {
      bbag_recover(tm);
    }

    Loc loc = redex_pop_loc(tm);
    if (loc) {
      busy = set_busy(busy);
      
      Pair pair = take_pair(loc);
      
      // If we just emptied a stolen bag, return it
      if (bbag_looted(tm)) {
        bbag_return(tm);
      }

      interact(tm, pair_neg(pair), pair_pos(pair));
    } else {
      if (busy && can_idle(tm)) {
        busy = set_idle(busy);
      }
      if (!try_steal(tm)) {
        sched_yield();
        if (!busy && timeout(tick))
          break;
      }
    }
  }

  #ifdef SUMMARY
  fprintf(stderr, "t%u itrs %" PRIu64 "\n", tm->tid, tm->itrs);
  #endif

  return NULL;
}

// --- Parallel normalization ---
static void parallel_normalize() {
  atomic_store_explicit(&net.idle, TPC-1, memory_order_relaxed);

  for (u64 i = 0; i < TPC; i++) {
    pthread_create(&threads[i], NULL, thread_func, (void*)i);
  }
  for (u64 i = 0; i < TPC; i++) {
    pthread_join(threads[i], NULL);
  }
}

// --- Core Normalization ---
Term normalize(Term term) {
  if (term_tag(term) != REF) {
    fprintf(stderr, "normalizing non-ref\n");
    exit(1);
  }

  boot(term_loc(term));

  if (TPC == 1) {
    TM *tm = tms[0];
    while (sequential_step(tm))
      ;
  } else {
    parallel_normalize();
  }

  return get(0);
}

// --- Debugging ---
static char *tag_to_str(Tag tag) {
  switch (tag) {
  case 0:
    return "___";
  case VAR:
    return "VAR";
  case SUB:
    return "SUB";
  case NUL:
    return "NUL";
  case ERA:
    return "ERA";
  case LAM:
    return "LAM";
  case APP:
    return "APP";
  case SUP:
    return "SUP";
  case DUP:
    return "DUP";
  case REF:
    return "REF";
  case OPX:
    return "OPX";
  case OPY:
    return "OPY";
  case U48:
    return "U48";
  case I48:
    return "I48";
  case F48:
    return "F48";
  case MAT:
    return "MAT";

  default:
    return "???";
  }
}

__attribute__((unused)) static const char *term_str(char *buf, Term term) {
  snprintf(buf, 64, "%s lab:%u loc:%lu", tag_to_str(term_tag(term)),
           term_lab(term), term_loc(term));
  return buf;
}

static void dump_term(Loc loc) {
  Term term = get(loc);
  printf("%04lu %03lu %03u %s\n", loc, term_loc(term), term_lab(term),
         tag_to_str(term_tag(term)));
}

void tm_dump_buff(TM *tm) {
  printf("------------------\n");
  printf("      NODES\n");
  printf("ADDR LOC LAB TAG\n");
  printf("------------------\n");
  for (Loc idx = 0; idx < tm->nput; idx++) {
    dump_term(rnod_ini(tm->tid) + idx);
  }
  printf("------------------\n");
  printf("    REDEX BAG\n");
  printf("ADDR   LOC LAB TAG\n");
  printf("------------------\n");
  for (Loc idx = 0; idx < tm->rput; idx++) {
    dump_term(rbag_ini(tm->tid) + idx);
  }
  printf("------------------\n");
  fflush(stdout);
}

void dump_buff() { tm_dump_buff(tms[0]); }
