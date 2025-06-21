// HVM3-Strict Core: parallel, polarized, LAM/APP & DUP/SUP only

#include <assert.h>
#include <execinfo.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include "macros.h"

#define SUMMARY
// #define DEBUG

#ifdef __APPLE__
extern void dmb_ishst(void);
__asm__(".global dmb_ishst\n"
        ".global _dmb_ishst\n"
        "_dmb_ishst:\n"
        "dmb_ishst:\n"
        "    dmb ishst\n"
        "    ret\n");
#endif

// Tags
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
#define U36 0x0C
#define I36 0x0D
#define F36 0x0E
#define MAT 0x0F

// Operations
#define OP_ADD 0x00
#define OP_SUB 0x01
#define OP_MUL 0x02
#define OP_DIV 0x03
#define OP_MOD 0x04
#define OP_EQ 0x05
#define OP_NE 0x06
#define OP_LT 0x07
#define OP_GT 0x08
#define OP_LTE 0x09
#define OP_GTE 0x0A
#define OP_AND 0x0B
#define OP_OR 0x0C
#define OP_XOR 0x0D
#define OP_LSH 0x0E
#define OP_RSH 0x0F

typedef u64 Term;     // [ Loc:54 | Lab:8 | Tag:4 ]
typedef u64 Loc; // 54 bits
typedef u8 Lab; // 8 bits
typedef u8 Tag;  // 4 bits

typedef u128 Pair;

#define TAG_BITS 4
#define LAB_BITS 8 
#define LOC_BITS 52 
#define TAG_MASK ((1ULL << TAG_BITS) - 1)
#define LAB_MASK ((1ULL << LAB_BITS) - 1)
#define LOC_MASK ((1ULL << LOC_BITS) - 1)

typedef union {
  u64 u;
  i64 i;
  f64 f;
} TypeConverter;

// Heap config
enum : u64 {
  HEAP_1GB = (1ULL << 27) * sizeof(u64),
  HEAP_SIZ = HEAP_1GB * 16,
  CACH_SIZ = 64,
  CACH_U64 = CACH_SIZ / sizeof(u64),
  TPC = 24, // threads per CPU

#ifdef __APPLE__
  PCOR_TOT = 4,
  ECOR_TOT = TPC - PCOR_TOT,
  PCOR = TPC < PCOR_TOT ? TPC : PCOR_TOT,
  ECOR = TPC > PCOR_TOT ? (TPC - PCOR) : 0,
#endif

  ZERO = 0,
  IDLE = 256,
  DFER_LEN = 256, // deferred bag terms per thread
  DFER_SYN = 32,  // sync threshold
  RBAG_QED = 8192 * TPC * sizeof(Pair),
  RBAG_SIZ = RBAG_QED,
};

enum : u64 {
  RBAG = ((HEAP_SIZ - RBAG_SIZ) / sizeof(Term)) & ~1ULL,
  RBAG_LEN = (RBAG_SIZ / (TPC * sizeof(Term))) & ~1ULL,
  NODE_LEN = (HEAP_SIZ - RBAG_SIZ) / (TPC * sizeof(Term)),
  BBAG_LEN = 96, // booty bag terms per thread
  DFER_INI = (RBAG_LEN - (DFER_LEN * 2)) & ~1ULL,
  RPUT_MAX = DFER_INI - BBAG_LEN
};

typedef struct Net {
  a64 idle;
} Net;

typedef struct Def {
  char *name;
  Term *nodes;
  u64 nodes_len;
  Term *rbag;
  u64 rbag_len;
} Def;

typedef struct Book {
  Def *defs;
  u32 len;
  u32 cap;
} Book;

// Thread memory
typedef struct TM {
  u16 tid;     // thread id
  Loc nput;    // next node allocation index
  Loc rput;    // next rbag push index
  Loc bput;    // booty bag push index
  u32 sid;     // stolen bbag thread id
  Loc spop;    // stolen bbag pop index + 2
  Loc dput[2]; // deferred bag push indices
  bool buse;   // can use booty bag
  bool bhld;   // booty bag is held
  u8 dpid;     // deferred bag index
  bool duse;   // can use deferred bag
  bool dsyn;   // deferred bag synced
  u8 lvic;     // last failed steal victim
#ifdef __APPLE__
  u8 pvic; // last failed PCore victim
  u8 evic; // last failed ECore victim
#endif
  u64 itrs; // interaction count
} TM;

// Booty bag control values
enum : u64 {
  HELD = 1,    // held by owner
  DROPPED = 2, // dropped, can be stolen
  STOLEN = 3,  // stolen by another thread
};

typedef struct BB {
  a64 ctrl; // control word
} BB;

// Globals
static u64 *BUFF = NULL;
static Net net;
static Book BOOK = {.defs = NULL, .len = 0, .cap = 0};
static TM *tms[TPC];
static BB bbs[TPC];
static pthread_t threads[TPC];

// Function declarations
static char *tag_to_str(Tag tag);
static const char *term_str(char *buf, Term term);

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
  expect(tm != NULL, "tm_new() allocation failed");  tm_reset(tm);
  tm->tid = tid;

  // Booty bag
  tm->bput = 0;
  tm->bhld = true;
  tm->buse = false;
  tm->sid = TPC;
  tm->spop = 0;

  // Deferred bag
  tm->dput[0] = 0;
  tm->dput[1] = 0;
  tm->dpid = 0;
  tm->duse = false;
  tm->dsyn = false;

  // Steal victims
  tm->lvic = TPC;
#ifdef __APPLE__
  tm->pvic = tid % PCOR;
  tm->evic = (tid % ECOR) + PCOR;
#endif

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

// Pair operations
static Pair pair_new(Term neg, Term pos) { return ((Pair)pos << 64) | neg; }

static Term pair_pos(Pair pair) { return (pair >> 64) & 0xFFFFFFFFFFFFFFFF; }

static Term pair_neg(Pair pair) { return pair & 0xFFFFFFFFFFFFFFFF; }

// Term operations (LOC TOP BITS)
// Term term_new(Tag tag, Lab lab, Loc loc) {
//   return (((Term)loc & LOC_MASK) << (LAB_BITS + TAG_BITS)) |
//          (((Term)lab & LAB_MASK) << TAG_BITS) |
//          ((Term)tag & TAG_MASK);
// }
// static Term term_with_loc(Term term, Loc loc) {
//   return (((Term)loc & LOC_MASK) << (LAB_BITS + TAG_BITS)) |
//          (term & ((1ULL << (LAB_BITS + TAG_BITS)) - 1));
// }
//
// u64 term_loc(Term term) { return (term >> (LAB_BITS + TAG_BITS)) & LOC_MASK; }
// u64 term_lab(Term term) { return (term >> TAG_BITS) & LAB_MASK; }
// Tag term_tag(Term term) { return term & TAG_MASK; }

Term term_new(Tag tag, Lab lab, Loc loc) {
    return (((Term)tag & TAG_MASK) << (LAB_BITS + LOC_BITS)) |
           (((Term)lab & LAB_MASK) << LOC_BITS) |
           ((Term)loc & LOC_MASK);
}

static Term term_with_loc(Term term, Loc loc) {
    return (term & ~LOC_MASK) | ((Term)loc & LOC_MASK);
}

u64 term_loc(Term term) { return term & LOC_MASK; }
u64 term_lab(Term term) { return (term >> LOC_BITS) & LAB_MASK; }
Tag term_tag(Term term) { return (term >> (LOC_BITS + LAB_BITS)) & TAG_MASK; }

static bool term_has_loc(Term term) {
  Tag tag = term_tag(term);
  return !(tag == SUB || tag == NUL || tag == ERA || tag == REF || tag == U36);
}

static Term term_offset_loc(Term term, Loc offset) {
  if (!term_has_loc(term)) {
    return term;
  }
  Loc loc = term_loc(term) + offset;
  return term_with_loc(term, loc);
}

static Loc port(u64 n, Loc loc) { return n + loc - 1; }

// Memory operations
Term swap(Loc loc, Term term) {
  return atomic_exchange_explicit((a64 *)&BUFF[loc], term,
                                  memory_order_relaxed);
}

Term take(Loc loc) {
  return atomic_exchange_explicit((a64 *)&BUFF[loc], ZERO,
                                  memory_order_relaxed);
}

Term get(Loc loc) {
  return atomic_load_explicit((a64 *)&BUFF[loc], memory_order_relaxed);
}

void set(Loc loc, Term term) {
  atomic_store_explicit((a64 *)&BUFF[loc], term, memory_order_relaxed);
}

static Pair take_pair(Loc loc) { return *(Pair *)&BUFF[loc]; }

static void set_pair(Loc loc, Pair pair) { *((Pair *)&BUFF[loc]) = pair; }

// Bag offsets and checks
static Loc bbag_offset(u32 tid) { return tid * RBAG_LEN; }
static Loc bbag_ini(u32 tid) { return RBAG + bbag_offset(tid); }
static bool bbag_empty(TM *tm) { return tm->bput == 0; }
static bool bbag_full(TM *tm) { return tm->bput == BBAG_LEN; }

static bool bbag_compare_swap(u32 tid, u64 expect, u32 desire,
                              memory_order success_order) {
  return atomic_compare_exchange_strong_explicit(
      &bbs[tid].ctrl, &expect, desire, success_order, memory_order_relaxed);
}

static void bbag_set(u32 tid, u32 val, memory_order order) {
  atomic_store_explicit(&bbs[tid].ctrl, val, order);
}

static u32 bbag_get(u32 tid) {
  return atomic_load_explicit(&bbs[tid].ctrl, memory_order_relaxed);
}

// Drop held, full booty bag (make stealable)
static void bbag_drop(TM *tm) {
  bbag_set(tm->tid, DROPPED, memory_order_release);
  tm->bhld = false;
}

// Recover stolen and emptied booty bag
static bool bbag_recover(TM *tm) {
  bool held = bbag_get(tm->tid) == HELD;
  if (held) {
    tm->bhld = true;
    tm->bput = 0;
  }
  return held;
}

// Pick up our own dropped booty bag
static bool bbag_pickup(TM *tm) {
  bool held = bbag_compare_swap(tm->tid, DROPPED, HELD, memory_order_relaxed);
  if (held) {
    tm->bhld = true;
    tm->spop = tm->bput; // always BBAG_LEN
    tm->sid = tm->tid;
  }
  return held;
}

// Steal another thread's dropped booty bag
static bool bbag_steal(TM *tm, u32 sid) {
  bool stole = bbag_compare_swap(sid, DROPPED, STOLEN, memory_order_acquire);
  if (stole) {
    tm->spop = BBAG_LEN;
    tm->sid = sid;
  }
  return stole;
}

static bool bbag_looted(TM *tm) { return (tm->sid < TPC) && (tm->spop == 0); }

// Return stolen, empty booty bag
static void bbag_return(TM *tm) {
  if (tm->sid != tm->tid) {
    bbag_set(tm->sid, HELD, memory_order_relaxed);
  }
  tm->sid = TPC;
}

static Loc rbag_ini(u32 tid) { return bbag_ini(tid) + BBAG_LEN; }
static bool rbag_empty(TM *tm) { return tm->rput == 0; }
static Loc rnod_ini(u32 tid) { return tid * NODE_LEN; }
static Loc dfer_offset(u32 idx) { return DFER_INI + DFER_LEN * idx; }
static Loc dfer_ini(u32 tid, u32 idx) {
  return bbag_ini(tid) + dfer_offset(idx);
}

// static bool dfer_empty(TM *tm) {
// return tm->dput[1u-tm->dpid] == 0;
// }

static bool dfer_any(TM *tm) { return (tm->dput[0] > 0) || (tm->dput[1] > 0); }

// Allocator
static Loc node_alloc(TM *tm, u32 cnt) {
  if (tm->nput + cnt >= NODE_LEN) {
    char msg[128];
    snprintf(msg, sizeof(msg), "%u node space exhausted, nput: %lu, cnt: %u, LEN: %lu", tm->tid, tm->nput, cnt, NODE_LEN);
    panic(msg);
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

  // Push to booty bag if not stealing and not full
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
    // Pushed to booty bag - drop if full and other bags available
    if (bbag_full(tm) && (!rbag_empty(tm) || dfer_any(tm))) {
      bbag_drop(tm);
    }
  }

#ifdef DEBUG
  if (off > BBAG_LEN && tm->rput >= RPUT_MAX - 1) {
    panic("rbag space exhausted");
  }
  // panic("rbag space exhausted");
#endif
}

static Loc dfer_pop_loc(TM *tm) {
  if (tm->dsyn) {
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
#endif
  tm->dpid = 1u - tm->dpid;
  tm->dsyn = true;
}

static Loc redex_pop_loc(TM *tm) {
  // Check deferred bags first
  if (tm->duse) {
    Loc loc = dfer_pop_loc(tm);
    if (loc > 0)
      return loc;

    // Sync deferred bag if threshold reached
    if (tm->dput[tm->dpid] > DFER_SYN) {
      dfer_sync(tm);
      loc = dfer_pop_loc(tm);
      if (loc > 0)
        return loc;
    }
  }

  if ((tm->sid < TPC) && (tm->spop > 0)) {
    // Pop from stolen booty bag
    tm->spop -= 2;
    if (tm->sid == tm->tid) {
      tm->bput -= 2;
    }
    return bbag_ini(tm->sid) + tm->spop;
  } else if (tm->rput > 0) {
    // Pop from RBAG
    tm->rput -= 2;
    return rbag_ini(tm->tid) + tm->rput;
  } else if (tm->duse && tm->dput[tm->dpid] > 0) {
    // Sync deferred bag with any elements
    dfer_sync(tm);
    return dfer_pop_loc(tm);
  }
  return 0; // Steal from someone else
}

// FFI functions
void hvm_init() {
  if(!BUFF) {
    BUFF = aligned_alloc(CACH_SIZ, HEAP_SIZ);
    if (BUFF == NULL) {
      panic("Heap allocation failed"); 
    }
  }
  memset(BUFF, 0, HEAP_SIZ);
  alloc_static_data();

#ifdef SUMMARY
  fprintf(stderr, "HEAP_SIZ = %" PRIu64 "\n", HEAP_SIZ);
  fprintf(stderr, "RBAG_SIZ = %" PRIu64 "\n", RBAG_SIZ);
  fprintf(stderr, "RBAG     = %lu\n", RBAG);
  fprintf(stderr, "RBAG_LEN = %lu\n", RBAG_LEN);
  fprintf(stderr, "NODE_LEN = %lu\n", NODE_LEN);
  fprintf(stderr, "BBAG_LEN = %lu\n", BBAG_LEN);
  fprintf(stderr, "DFER_INI = %lu\n", DFER_INI);
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

void handle_failure() {}

Loc ffi_alloc_node(u64 arity) {
  TM *tm = tms[0];
  Loc loc = tm->nput;
  tm->nput += arity;
  return loc;
}

void ffi_rbag_push(Term neg, Term pos) { redex_push(tms[0], neg, pos, false); }

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
    panic("def_new() memory allocation failed");
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
  const Def *def = &BOOK.defs[def_idx];
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
    Term pos = term_offset_loc(rbag[i + 1], offset);
    redex_push(tm, neg, pos, false);
  }
  return root;
}

static void boot(Loc def_idx) {
  TM *tm = tms[0];
  if (tm->nput > 0 || tm->rput > 0) {
    panic("booting on non-empty state");
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

// Interactions
static void interact_appref(TM *tm, Term neg, Loc pos_loc) {
  Term lam = expand_ref(tm, pos_loc);
#ifdef DEBUG
  if (term_tag(lam) != LAM) {
    // Assumption broken. May not matter, but I want to know.
    char msg[128];
    snprintf(msg, sizeof(msg), "APPREF root node is not a LAM, %s", tag_to_str(term_tag(lam)));
    panic(msg);  }
#endif
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

static void interact_appu32(TM *tm, Loc a_loc, u64 num) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(U36, 0, num), arg);
  move(tm, ret, term_new(U36, 0, num));
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
// u32 u32_to_u32(u32 u) { return u; }

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

u64 i64_to_u64(i64 i) {
  TypeConverter converter;
  converter.i = i;
  return converter.u;
}

u64 f64_to_u64(f64 f) {
  TypeConverter converter;
  converter.f = f;
  return converter.u;
}

static void interact_opynum(TM *tm, Loc a_loc, Lab op, u64 y, Tag y_type) {
  u64 x = term_loc(take(port(1, a_loc)));
  Loc ret = port(2, a_loc);
  u64 res = 0;

  if (y_type == U36) {
    static void *op_jumptable[] = {
        [OP_ADD] = &&do_add, [OP_SUB] = &&do_sub, [OP_MUL] = &&do_mul,
        [OP_DIV] = &&do_div, [OP_EQ] = &&do_eq,   [OP_NE] = &&do_ne,
        [OP_LT] = &&do_lt,   [OP_GT] = &&do_gt,   [OP_LTE] = &&do_lte,
        [OP_GTE] = &&do_gte, [OP_MOD] = &&do_mod, [OP_AND] = &&do_and,
        [OP_OR] = &&do_or,   [OP_XOR] = &&do_xor, [OP_LSH] = &&do_lsh,
        [OP_RSH] = &&do_rsh};

    // Faster branching
    goto *op_jumptable[op];

  do_add:
    res = x + y;
    goto done;
  do_sub:
    res = x - y;
    goto done;
  do_mul:
    res = x * y;
    goto done;
  do_div:
    res = x / y;
    goto done;
  do_eq:
    res = x == y;
    goto done;
  do_ne:
    res = x != y;
    goto done;
  do_lt:
    res = x < y;
    goto done;
  do_gt:
    res = x > y;
    goto done;
  do_lte:
    res = x <= y;
    goto done;
  do_gte:
    res = x >= y;
    goto done;
  do_mod:
    res = x % y;
    goto done;
  do_and:
    res = x & y;
    goto done;
  do_or:
    res = x | y;
    goto done;
  do_xor:
    res = x ^ y;
    goto done;
  do_lsh:
    res = x << y;
    goto done;
  do_rsh:
    res = x >> y;
    goto done;

  done:;
  } else {
    // Inlined type conversion and operation for i36 and f36
    switch (y_type) {
    case I36: {
      i32 a = (x);
      i32 b = u64_to_i64(y);
      i32 val;
      switch (op) {
      case OP_ADD:
        val = a + b;
        break;
      case OP_SUB:
        val = a - b;
        break;
      case OP_MUL:
        val = a * b;
        break;
      case OP_DIV:
        val = a / b;
        break;
      case OP_EQ:
        val = a == b;
        break;
      case OP_NE:
        val = a != b;
        break;
      case OP_LT:
        val = a < b;
        break;
      case OP_GT:
        val = a > b;
        break;
      case OP_LTE:
        val = a <= b;
        break;
      case OP_GTE:
        val = a >= b;
        break;
      case OP_MOD:
        val = a % b;
        break;
      case OP_AND:
        val = a & b;
        break;
      case OP_OR:
        val = a | b;
        break;
      case OP_XOR:
        val = a ^ b;
        break;
      case OP_LSH:
        val = a << b;
        break;
      case OP_RSH:
        val = a >> b;
        break;
      default:
        val = 0;
      }
      res = i64_to_u64(val);
      break;
    }
    case F36: {
      f64 a = u64_to_f64(x);
      u64 b = u64_to_f64(y);
      f64 val;
      switch (op) {
      case OP_ADD:
        val = a + b;
        break;
      case OP_SUB:
        val = a - b;
        break;
      case OP_MUL:
        val = a * b;
        break;
      case OP_DIV:
        val = a / b;
        break;
      case OP_EQ:
        val = a == b;
        break;
      case OP_NE:
        val = a != b;
        break;
      case OP_LT:
        val = a < b;
        break;
      case OP_GT:
        val = a > b;
        break;
      case OP_LTE:
        val = a <= b;
        break;
      case OP_GTE:
        val = a >= b;
        break;
      default:
        val = 0;
      }
      res = f64_to_u64(val);
      break;
    }
    }
  }
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

static void interact_dupnum(TM *tm, Loc a_loc, u64 n, Tag n_type) {
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

static void interact_matnum(TM *tm, Loc mat_loc, Lab mat_len, u64 n,
                            Tag n_type) {
  if (n_type != U36) {
    panic("match with non-U36");
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
    set(app + 0, term_new(U36, 0, n - (mat_len - 1)));
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
    case U36:
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
    case U36:
    case I36:
    case F36:
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
    case U36:
    case I36:
    case F36:
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
    case U36:
    case I36:
    case F36:
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
    case U36:
    case I36:
    case F36:
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
    case U36:
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

static bool sequential_step(TM *tm) {
  Loc loc = redex_pop_loc(tm);
  if (loc == 0)
    return false;
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

static u32 get_victim(TM *tm) {
#ifdef __APPLE__
  // PCore bias: if we didn't fail last attempt, or we failed on ECore, try
  // PCore
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
  if (!tm->buse)
    return false;

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
      if (bbag_pickup(tm))
        return true;
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
    if (idle == TPC)
      return true;
  }
  return false;
}

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
#endif

static void *thread_func(void *arg) {
  int thread_id = (u64)arg;

#ifdef __APPLE__
  bind_core(thread_id);
#endif
  TM *tm = tms[thread_id];

  // Wait until after injection to turn these on
  tm->buse = true;
  tm->duse = true;

  u64 tick = 0;
  bool busy = tm->tid == 0;
  while (true) {
    tick += 1;

    // If we know we're not holding our own booty bag, it may have been stolen
    // and returned
    // Try to recover it
    if (!tm->bhld)
      bbag_recover(tm);

    Loc loc = redex_pop_loc(tm);
    if (loc) {
      busy = set_busy(busy);
      Pair pair = take_pair(loc);

      // If we just emptied a stolen bag, return it
      if (bbag_looted(tm))
        bbag_return(tm);

      interact(tm, pair_neg(pair), pair_pos(pair));
    } else {
      if (busy && can_idle(tm))
        busy = set_idle(busy);
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

static void parallel_normalize() {
  atomic_store_explicit(&net.idle, TPC - 1, memory_order_relaxed);

  for (u64 i = 0; i < TPC; i++) {
    pthread_create(&threads[i], NULL, thread_func, (void *)i);
  }
  for (u64 i = 0; i < TPC; i++) {
    pthread_join(threads[i], NULL);
  }
}

Term normalize(Term term) {
  if (term_tag(term) != REF) {
    panic("normalizing non-ref");  
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

// Debugging
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
  case U36:
    return "U36";
  case I36:
    return "I36";
  case F36:
    return "F36";
  case MAT:
    return "MAT";
  default:
    return "???";
  }
}

__attribute__((unused)) static const char *term_str(char *buf, Term term) {
  snprintf(buf, 64, "%s lab:%lu loc:%lu", tag_to_str(term_tag(term)),
           term_lab(term), term_loc(term));
  return buf;
}

static void dump_term(Loc loc) {
  Term term = get(loc);
  printf("%04lu %03lu %03lu %s\n", loc, term_loc(term), term_lab(term),
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
