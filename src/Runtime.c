// HVM3-Strict Core: parallel, polarized, LAM/APP & DUP/SUP only

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

//#define SUMMARY

//#define DEBUG
//#define VOIDTEST

#define DEBUG_LOG(fmt, ...) fprintf(stderr, "[DEBUG] " fmt "\n", ##__VA_ARGS__)

int get_num_threads() {
  long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  return (nprocs <= 1) ? 1 : 1 << (int)log2(nprocs);
}

void exit_stacktrace() {
  void *array[10];
  size_t size;
  
  size = backtrace(array, 10);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

void segv_handler(int sig) {
  void *array[10];
  size_t size;
  
  size = backtrace(array, 10);
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

#ifdef __APPLE__
// Use explicit file-scope assembly to prevent compiler from optimizing out
extern uint64_t read_cntvct(void);
__asm__(
    ".global _read_cntvct\n"
    ".global read_cntvct\n"
    "_read_cntvct:\n"
    "read_cntvct:\n"
    "   mrs x0, cntvct_el0\n"
    "   ret\n"
);

extern void dmb_ishst(void);
__asm__(
    ".global dmb_ishst\n"
    ".global _dmb_ishst\n"
    "_dmb_ishst:\n"
    "dmb_ishst:\n"
    "    dmb ishst\n"
    "    ret\n"
);
#endif

// Constants
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
#define U32 0x0C
#define I32 0x0D
#define F32 0x0E
#define MAT 0x0F

// Operators
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

// Types
typedef uint8_t      u8;
typedef int32_t      i32;
typedef uint32_t     u32;
typedef _Atomic(u32) a32;
typedef float        f32;
typedef int64_t      i64;
typedef uint64_t     u64;
typedef _Atomic(u64) a64;
typedef unsigned __int128 u128 __attribute__((aligned(16)));

typedef u8   Tag;  //  8 bits
typedef u32  Lab;  // 24 bits
typedef u32  Loc;  // 32 bits
typedef u64  Term; // Loc | Lab | Tag
typedef u128 Pair; // Term | Term

// Using union for type punning (safer in C)
typedef union {
  u32 u;
  i32 i;
  f32 f;
} TypeConverter;

// Global heap
static u64 *BUFF = NULL;

// Heap configuration options
enum : u64 {
  // 1GiB heap
  HEAP_1GB = (1ULL << 27) * sizeof(u64),  // 128Mi * 8 bytes = 1GiB

  //////////////////////////
  // Choose a heap size here
  //////////////////////////
  HEAP_SIZ = HEAP_1GB * 4,

  // Cache line size
  CACH_SIZ = 64,
  CACH_U64 = CACH_SIZ / sizeof(u64),
  ZERO = 0,

  // Threads per CPU
  TPC = 10,

  // Overflow bag terms per thread (there are 2 bags)
  OFLW_LEN = 256,
  // When overflow bag gets this big, a memory sync occurs
  OFLW_SYNC = 32,

  // Includes overflow and BBAG
  RBAG_RANDO = 8192 * TPC * sizeof(Pair),

  // Used in other calculations below
  RBAG_SIZ = RBAG_RANDO,
};

enum : u32 {
  // Most of these must be 16-byte aligned (even numbers).

  // Final calculated RBAG index
  RBAG = ((HEAP_SIZ - RBAG_SIZ) / sizeof(Term)) & ~1ULL,

  // Calculate RBAG_LEN and NODE_LEN as terms per thread
  RBAG_LEN = (RBAG_SIZ / (TPC * sizeof(Term))) & ~1ULL,
  NODE_LEN = (HEAP_SIZ - RBAG_SIZ) / (TPC * sizeof(Term)),

  // Booty bag terms per thread
  BBAG_LEN = 96,

  // Starting offset in RBAG of overflow bags
  OFLW_INI = (RBAG_LEN - (OFLW_LEN * 2)) & ~1ULL,

  // Max terms allowed in RBAG, taking into account booty bag and overflow
  RPUT_MAX = OFLW_INI - BBAG_LEN
};

typedef struct Net {
  a64 nods;
  a64 itrs;
  a32 alignas(64) idle;  // TODO Probably should just make it a64
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

// TODO: net_reset()
static Net net = {
  .nods = 0,
  .itrs = 0,
  .idle = 0
};

static Book BOOK = {
  .defs = NULL,
  .len = 0,
  .cap = 0,
};

#define MBUF_SIZ 10

// Local Thread Memory
typedef struct TM {
  u32 tid;   // thread id
  Loc nput;  // next node allocation attempt index
  Loc rput;  // next rbag push index
  u32 sid;   // tid from which bbag was stolen
  Loc bput;  // owned bbag push index
  Loc spop;  // stolen bbag pop index + 2

  u32 sgud;  // successful steal count
  u32 sbad;  // failed steal count

  Loc oput[2]; // next oflw bag push indices
  
  bool buse; // can use booty bag
  bool bhld; // when we KNOW booty bag ctrl word is HELD
             // (in some cases it may be HELD but we don't know)

  u8   opid; // oput idx we are pushing into
  bool ouse; // can use overflow
  bool osyn; // overflow is sync'd

  u64 itrs;  // interaction count
} TM;

static_assert(sizeof(TM) <= CACH_SIZ, "TM struct getting big");

// Booty bag control word values
enum : u32 {
  HELD = 1,    // held by owner, or "tossed" by another thread
  DROPPED = 2, // dropped by owner. can be picked back up by owner
               // or stolen by another thread
  STOLEN = 3,  // stolen by another thread
};

typedef struct BB {
  a32 alignas(64) ctrl; // control word TODO: just make it a64
} BB;

static TM *tms[TPC];
static BB bbs[TPC];
static pthread_t threads[TPC];
#ifdef DEBUG
static uint8_t unprocessed_itrs[255] = { 0 };
#endif

static _Thread_local int thread_id = 0;

// Debugging
static char *tag_to_str(Tag tag);

static Term pair_pos(Pair pair);
static Term pair_neg(Pair pair);
static const char* term_str(char* buf, Term term);

// FFI functions
void dump_buff();
Tag term_tag(Term term) { return term & 0xFF; }
Loc term_loc(Term term);

static u32 u64_hi(u64 e) {
  return e >> 32;
}

__attribute((unused))
static u32 u64_lo(u64 e) {
  return e & 0xFFFFFFFF;
}

// TM/BB operations
void tm_reset(TM *tm) {
  tm->rput = 0;
  tm->nput = 0;
  tm->itrs = 0;
}

TM *tm_new(u64 tid) {
  // make size a multiple of alignment
  size_t tm_siz = (sizeof(TM) + CACH_SIZ - 1) & ~(CACH_SIZ - 1);
  TM *tm = aligned_alloc(CACH_SIZ, tm_siz);
  if (tm == NULL) {
    fprintf(stderr, "TM memory allocation failed\n");
    exit(EXIT_FAILURE);
  }
  tm_reset(tm);
  tm->tid = tid;
  tm->bput = 0;
  tm->spop = 0;
  tm->sid = TPC;
  tm->sgud = 0;
  tm->sbad = 0;
  tm->bhld = true;
  tm->buse = false;

  tm->oput[0] = 0;
  tm->oput[1] = 0;
  tm->opid = 0;
  tm->ouse = false;
  tm->osyn = false;

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
    #if 0
    if (bbs[t] != NULL) {
      free(bbs[t]);
      bbs[t] = NULL;
    }
    #endif
  }
}

// Pair operations
static Pair pair_new(Term neg, Term pos) {
  return ((Pair)pos << 64) | neg;
}

static Term pair_pos(Pair pair) {
  return (pair >> 64) & 0xFFFFFFFFFFFFFFFF;
}

static Term pair_neg(Pair pair) {
  return pair & 0xFFFFFFFFFFFFFFFF;
}

// Term operations
Term term_new(Tag tag, Lab lab, Loc loc) {
  return ((Term)loc << 32) | ((Term)lab << 8) | tag;
}

Lab term_lab(Term term) { return (term >> 8) & 0xFFFFFF; }

Loc term_loc(Term term) { return u64_hi(term); }

static Term term_with_loc(Term term, Loc loc) {
  return (((Term)loc) << 32) | (term & 0xFFFFFFFF);
}

static bool term_has_loc(Term term) {
  Tag tag = term_tag(term);
  return !(tag == SUB || tag == NUL || tag == ERA || tag == REF || tag == U32);
}

static Term term_offset_loc(Term term, Loc offset) {
  if (!term_has_loc(term)) { return term; }
  Loc loc = term_loc(term) + offset;
  return term_with_loc(term, loc);
}

__attribute__((unused))
static int bty_debug = 0; // booty bag
__attribute__((unused))
static int oflw_debug = 0; // overflow

__attribute__((unused))
static const char* term_str(char* buf, Term term) {
  snprintf(buf, 64, "%s lab:%u loc:%u", tag_to_str(term_tag(term)),
          (u32)term_lab(term), (u32)term_loc(term));
  return buf;
}

// Memory operations
Term swap_lvl(Loc loc, Term term, u32 lvl) {
  Term got = atomic_exchange_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);

  #ifdef VOIDTEST
  if (got == 0) {
    fprintf(stderr, "%d swap got NULL @ %u\n", thread_id, loc);
    exit_stacktrace();
  }
  #endif
  return got;
}

Term swap(Loc loc, Term term) {
  return swap_lvl(loc, term, 0);
}

Term take(Loc loc) {
  Term term = atomic_exchange_explicit((a64*)&BUFF[loc], ZERO, memory_order_relaxed);

  #ifdef VOIDTEST
  if (term == 0) {
    fprintf(stderr, "%d take got NULL @ %u\n", thread_id, loc);
    exit_stacktrace();
  }
  #endif
  return term;
}

Term get(Loc loc) {
  Term term = atomic_load_explicit((a64*)&BUFF[loc], memory_order_relaxed);
  return term;
}

void set(Loc loc, Term term) {
  atomic_store_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);

}

static Pair take_pair(Loc loc) {
  Pair pair = *(Pair*)&BUFF[loc];
  
  // Debugging
  #if defined(VOIDTEST)
  Term neg = pair_neg(pair);
  Term pos = pair_pos(pair);
  //*(Pair*)&BUFF[loc] = (Pair)ZERO;
  if ((neg == 0) || (pos == 0)) {
    fprintf(stderr, "%d take_pair: void term taken\n", thread_id);
    exit_stacktrace();
  }
  #endif
  
  return pair;
}

static void set_pair(Loc loc, Pair pair) {
  #ifdef VOIDTEST
  Term neg = pair_neg(pair);
  Term pos = pair_pos(pair);
  if ((neg == 0) || (pos == 0)) {
    fprintf(stderr, "%d set_pair: void term\n", thread_id);
    exit_stacktrace();
  }
  #endif

  *((Pair*)&BUFF[loc]) = pair;
}

static Loc port(u64 n, Loc loc) { return n + loc - 1; }

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

static bool bbag_compare_swap(u32 tid, u32 expect, u32 desire,
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
  #if defined(DEBUG)
  if (bty_debug) {
    fprintf(stderr, "%u dropping bbag! bput %u rput %u\n",
            tm->tid, tm->bput, tm->rput);
  }
  #endif

  bbag_set(tm->tid, DROPPED, memory_order_release);
  tm->bhld = false;
}

// Attempt to recover a tossed (stolen, and emptied) booty bag
static bool bbag_recover(TM *tm) {
  bool held = bbag_get(tm->tid) == HELD;
  if (held) {
    tm->bhld = true;
    tm->bput = 0;

    #if defined(DEBUG)
    if (bty_debug) {
      fprintf(stderr, "%u recovered empty stolen bbag!\n", tm->tid);
    }
    #endif
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

    #if defined(DEBUG)
    if (bty_debug) {
      fprintf(stderr, "%d picked up our own dropped bbag!\n", tm->tid);
    }
    #endif
  }
  return held;
}

// Attempt to steal another thread's dropped booty bag
static bool bbag_steal(TM *tm, u32 sid) {
  bool stole = bbag_compare_swap(sid, DROPPED, STOLEN, memory_order_acquire);
  if (stole) {
    tm->spop = BBAG_LEN;
    tm->sid = sid;
    
    #if defined(DEBUG)
    if (bty_debug) {
      fprintf(stderr, "%d stole t%u's bbag!\n", tm->tid, sid);
    }
    #endif
  }
  return stole;
}

// Toss the booty bag we stole from another thread and emptied
static void bbag_toss(TM *tm) {
  #if defined(DEBUG)
  if (bty_debug) {
    fprintf(stderr, "%u tossing t%u's empty stolen bbag!\n", tm->tid, tm->sid);
  }
  #endif

  bbag_set(tm->sid, HELD, memory_order_relaxed);
  tm->sid = TPC;
}

static Loc rbag_ini(u32 tid) {
  return bbag_ini(tid) + BBAG_LEN;
}

static bool rbag_empty(TM *tm) {
  return tm->rput == 0;
}

static Loc rnod_ini(u32 tid) {
  return tid * NODE_LEN;
}

static Loc oflw_offset(u32 oidx) {
  return OFLW_INI + OFLW_LEN * oidx;
}

static Loc oflw_ini(u32 tid, u32 oidx) {
  return bbag_ini(tid) + oflw_offset(oidx);
}

static bool oflw_empty(TM *tm) {
  return (tm->oput[0] == 0) && (tm->oput[1] == 0);
}  

// Allocator
// ---------

static u64 align(u64 align, u64 val) {
  return (val + align - 1) & ~(align - 1);
}

static Loc node_alloc(TM *tm, u32 cnt) {
  if (tm->nput + cnt >= NODE_LEN) {
    fprintf(stderr, "%u node space exhausted, nput: %u, cnt: %u, LEN: %u\n",
            tm->tid, tm->nput, cnt, NODE_LEN);
    exit(1);
  }

  Loc loc = rnod_ini(tm->tid) + tm->nput;
  tm->nput += cnt;
  return loc;
}

static Loc get_push_offset(TM *tm, bool force_oflw) {
  if (force_oflw && tm->ouse) {
    #if defined(DEBUG)
    if (oflw_debug) {
      fprintf(stderr, "%u pushing to overflow %u @ %u\n", tm->tid,
            0u+tm->opid, tm->oput[tm->opid]);
    }
    #endif

    u32 oput = tm->oput[tm->opid];
    tm->oput[tm->opid] += 2;
    return oflw_offset(tm->opid) + oput;
  }

  // Only push to booty bag if we aren't stealing
  if (tm->buse && tm->bhld && !bbag_full(tm) && (tm->sid == TPC)) {
    #if defined(DEBUG)
    if (bty_debug) {
      fprintf(stderr, "%u pushing to booty bag @ %u\n", tm->tid, tm->bput);
    }
    #endif

    u32 bput = tm->bput;
    tm->bput += 2;
    return bput;
  }

  #if 0 && defined(DEBUG)
  fprintf(stderr, "%u pushing to RBAG @ %u buse %u\n", tm->tid,
          tm->rput, (u32)tm->buse);
  #endif
  // Push to RBAG
  u32 rput = tm->rput;
  tm->rput += 2;
  return BBAG_LEN + rput;
}

static void rbag_push(TM *tm, Term neg, Term pos, bool force_oflw) {
  #ifdef VOIDTEST
  if ((neg == 0) || (pos == 0)) {
    fprintf(stderr, "%d rbag_push: void term\n", thread_id);
    exit_stacktrace();
  }
  #endif

  Loc off = get_push_offset(tm, force_oflw);
  Loc loc = bbag_ini(tm->tid) + off;

  set_pair(loc, pair_new(neg, pos));

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

_Thread_local u64 noflw = 0;

static Loc oflw_pop_loc(TM *tm) {
  if (tm->osyn) {
    // Overflow is sync'd so we can pop
    u32 oidx = 1u - tm->opid;
    if (tm->oput[oidx] > 0) {
      tm->oput[oidx] -= 2;
      
      #if defined(DEBUG)
      if (oflw_debug) {
        fprintf(stderr, "%u popping from overflow %u @ %u\n", tm->tid,
                oidx, tm->oput[oidx]);
      }
      #endif
      ++noflw;
      
      return oflw_ini(tm->tid, oidx) + tm->oput[oidx];
    } else {
      tm->osyn = false;
    }
  }
  return 0;
}

static void oflw_sync(TM *tm) {
#ifdef __APPLE__
  dmb_ishst();
#endif
  tm->opid = 1u - tm->opid;
  tm->osyn = true;
}

static Loc redex_pop_loc(TM* tm) {
  // Check overflow bag first when sync'd
  if (tm->ouse) {
    Loc loc = oflw_pop_loc(tm);
    if (loc > 0) return loc;

    if (tm->oput[tm->opid] > OFLW_SYNC) {
      oflw_sync(tm);
      Loc loc = oflw_pop_loc(tm);
      if (loc > 0) return loc;
    }
  }
    
  if ((tm->sid < TPC) && (tm->spop > 0)) {
    // Pop from stolen non-empty booty bag - but it may be our HELD bag
    tm->spop -= 2;

    // If we are stealing from our own bag, adjust push index as well
    if (tm->sid == tm->tid) {
      tm->bput -= 2;
    }

    #if defined(DEBUG)
    if (bty_debug) {
      fprintf(stderr, "%u popping from booty bag @ %u\n", tm->tid, tm->spop);
    }
    #endif

    return bbag_ini(tm->sid) + tm->spop;
  } else if (tm->rput > 0) {
    // Pop from non-empty RBAG
    tm->rput -= 2;

    #if 0 && defined(DEBUG)
    fprintf(stderr, "%u popping from RBAG @ %u\n", tm->tid, tm->rput);
    #endif

    return rbag_ini(tm->tid) + tm->rput;
  } else if (tm->ouse) {
    // Finally, try a flip & sync of overflow bags
    if (tm->oput[tm->opid] > 0) {
      oflw_sync(tm);
      Loc loc = oflw_pop_loc(tm);
      if (loc > 0) return loc;
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
      exit(EXIT_FAILURE);
    }
  }
  memset(BUFF, 0, HEAP_SIZ);

  alloc_static_data();

  #if 1
  fprintf(stderr, "HEAP_SIZ = %" PRIu64 "\n", HEAP_SIZ);
  fprintf(stderr, "RBAG_SIZ = %" PRIu64 "\n", RBAG_SIZ);
  fprintf(stderr, "RBAG     = %u\n", RBAG);
  fprintf(stderr, "RBAG_LEN = %u\n", RBAG_LEN);
  fprintf(stderr, "NODE_LEN = %u\n", NODE_LEN);
  fprintf(stderr, "BBAG_LEN = %u\n", BBAG_LEN);
  fprintf(stderr, "OFLW_INI = %u\n", OFLW_INI);
  fprintf(stderr, "OFLW_LEN = %" PRIu64 "\n", OFLW_LEN);
  #endif

  #ifdef DEBUG
  signal(SIGSEGV, segv_handler);
  #endif
}

void hvm_free() {
  if (BUFF != NULL) {
    free(BUFF);
    BUFF = NULL;
  }
  free_static_data();
}

Loc ffi_alloc_node(u64 arity) {
  TM *tm = tms[0];
  Loc loc = tm->nput;
  tm->nput += arity;
  return loc;
}

void ffi_rbag_push(Term neg, Term pos) {
  rbag_push(tms[0], neg, pos, false);
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

  u64 rbag_siz = align(CACH_SIZ, sizeof(Term) * rbag_cnt);
  u64 rnod_siz = align(CACH_SIZ, sizeof(Term) * rnod_cnt);

  Def def = {
      .name = name,
      .nodes = aligned_alloc(CACH_SIZ, rnod_siz),
      .nodes_len = rnod_cnt,
      .rbag = aligned_alloc(CACH_SIZ, rbag_siz),
      .rbag_len = rbag_cnt,
  };

  if ((def.nodes == NULL) || (def.rbag == NULL)) {
    fprintf(stderr, "DEF memory allocation failed\n");
    exit(1);
  }

  Term *nodes = BUFF;
  Term *rbag = &BUFF[rbag_ini(0)];

  memcpy(def.nodes, nodes, sizeof(Term) * def.nodes_len);
  memcpy(def.rbag, rbag, sizeof(Term) * def.rbag_len);

  #if 0
  printf("NEW DEF '%s':\n", def.name);
  dump_buff();
  printf("\n");
  #endif

  memset(BUFF, 0, sizeof(Term) * def.nodes_len);
  memset(rbag, 0, sizeof(Term) * def.rbag_len);

  tm_reset(tm);

  BOOK.defs[BOOK.len] = def;
  BOOK.len++;
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
    rbag_push(tm, neg, pos, false);
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
static inline void move_lvl(TM *tm, Loc neg_loc, Term pos, u32 lvl);

static inline void link_lvl(TM *tm, Term neg, Term pos, u32 lvl) {
  if (term_tag(pos) == VAR) {
    Term far = swap_lvl(term_loc(pos), neg, lvl);
    if (term_tag(far) != SUB) {
      move_lvl(tm, term_loc(pos), far, lvl + 1);
    }
  } else {
    rbag_push(tm, neg, pos, false);
  }
}

static inline void move_lvl(TM *tm, Loc neg_loc, Term pos, u32 lvl) {
  Term neg = swap_lvl(neg_loc, pos, lvl);
  if (term_tag(neg) != SUB) {
    // No need to take() since we already swapped
    link_lvl(tm, neg, pos, lvl + 1);
  }
}

static inline void link_terms(TM *tm, Term neg, Term pos) {
  link_lvl(tm, neg, pos, 0);
}

static inline void move(TM *tm, Loc neg_loc, Term pos) {
  move_lvl(tm, neg_loc, pos, 0);
}

// Interactions
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
  // Force push to overflow buffer
  rbag_push(tm, neg, lam, true);
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
  link_terms(tm, term_new(U32, 0, num), arg);
  move(tm, ret, term_new(U32, 0, num));
}

static void interact_opxnul(TM *tm, Loc a_loc) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  link_terms(tm, term_new(ERA, 0, 0), arg);
  move(tm, ret, term_new(NUL, 0, 0));
}

static void interact_opxnum(TM *tm, Loc loc, Lab op, u32 num, Tag num_type) {
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
u32 u32_to_u32(u32 u) { return u; }

i32 u32_to_i32(u32 u) {
  TypeConverter converter;
  converter.u = u;
  return converter.i;
}

f32 u32_to_f32(u32 u) {
  TypeConverter converter;
  converter.u = u;
  return converter.f;
}

u32 i32_to_u32(i32 i) {
  TypeConverter converter;
  converter.i = i;
  return converter.u;
}

u32 f32_to_u32(f32 f) {
  TypeConverter converter;
  converter.f = f;
  return converter.u;
}

static void interact_opynum(TM *tm, Loc a_loc, Lab op, u32 y, Tag y_type) {
  u32 x = term_loc(take(port(1, a_loc)));
  Loc ret = port(2, a_loc);
  u32 res = 0;

  // Optimized path using jump table & direct compute
  if (y_type == U32) {
    // Faster operation dispatch
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
    // Inlined type conversion and operation for i32 and f32
    switch (y_type) {
    case I32: {
      i32 a = u32_to_i32(x);
      i32 b = u32_to_i32(y);
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
      res = i32_to_u32(val);
      break;
    }
    case F32: {
      f32 a = u32_to_f32(x);
      f32 b = u32_to_f32(y);
      f32 val;
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
      res = f32_to_u32(val);
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
  fprintf(stderr, "interact_matnul not supported (yet)\n");
  exit(1);
  move(tm, port(1, a_loc), term_new(NUL, 0, 0));
  for (u32 i = 0; i < mat_len; i++) {
    link_terms(tm, term_new(ERA, 0, 0), take(port(i + 2, a_loc)));
  }
}

static void interact_matnum(TM *tm, Loc mat_loc, Lab mat_len, u32 n, Tag n_type) {
  if (n_type != U32) {
    fprintf(stderr, "match with non-U32\n");
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
    set(app + 0, term_new(U32, 0, n - (mat_len - 1)));
    set(app + 1, term_new(SUB, 0, 0));
    move(tm, ret, term_new(VAR, 0, port(2, app)));

    link_terms(tm, term_new(APP, 0, app), arm);
  }
}

static void interact_matsup(TM *tm, Loc mat_loc, Lab mat_len, Loc sup_loc) {
  fprintf(stderr, "interact_matsup not supported (yet)\n");
  exit(1);
  // TODO: convert to node_alloc( 2 + mat_len * 3)

  /*
  Loc ma0 = alloc_node(1 + mat_len);
  Loc ma1 = alloc_node(1 + mat_len);
  Loc sup = alloc_node(2);

  set(port(1, sup), term_new(VAR, 0, port(1, ma1)));
  set(port(2, sup), term_new(VAR, 0, port(1, ma0)));
  set(port(1, ma0), term_new(SUB, 0, 0));
  set(port(1, ma1), term_new(SUB, 0, 0));

  for (u64 i = 0; i < mat_len; i++) {
    Loc dui = alloc_node(2);
    set(port(1, dui), term_new(SUB, 0, 0));
    set(port(2, dui), term_new(SUB, 0, 0));
    set(port(2 + i, ma0), term_new(VAR, 0, port(2, dui)));
    set(port(2 + i, ma1), term_new(VAR, 0, port(1, dui)));

    link_terms(tm, term_new(DUP, 0, dui), take(port(2 + i, mat_loc)));
  }

  move(tm, port(1, mat_loc), term_new(SUP, 0, sup));
  link_terms(tm, term_new(MAT, mat_len, ma0), take(port(2, sup_loc)));
  link_terms(tm, term_new(MAT, mat_len, ma1), take(port(1, sup_loc)));
  */
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

static char *tag_to_str(Tag tag);

static bool interact(TM *tm, Term neg, Term pos) {
  Tag neg_tag = term_tag(neg);
  Tag pos_tag = term_tag(pos);
  Loc neg_loc = term_loc(neg);
  Loc pos_loc = term_loc(pos);

  //bool processed = true;

  switch (neg_tag) {
  case APP:
    switch (pos_tag) {
    case LAM:
      interact_applam(tm, neg_loc, pos_loc);
      break;
    case NUL:
      interact_appnul(tm, neg_loc);
      break;
    case U32:
      interact_appu32(tm, neg_loc, pos_loc);
      break;
    case REF:
      interact_appref(tm, neg, pos_loc);
      //link_terms(tm, neg, expand_ref(tm, pos_loc);
      break;
    case SUP:
      interact_appsup(tm, neg_loc, pos_loc);
      break;
    default:
      //processed = false;
      break;
    }
    break;
  case OPX:
    switch (pos_tag) {
    case NUL:
      interact_opxnul(tm, neg_loc);
      break;
    case U32:
    case I32:
    case F32:
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
      //processed = false;
      break;
    }
    break;
  case OPY:
    switch (pos_tag) {
    case NUL:
      interact_opynul(tm, neg_loc);
      break;
    case U32:
    case I32:
    case F32:
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
      //processed = false;
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
    case U32:
    case I32:
    case F32:
      interact_dupnum(tm, neg_loc, pos_loc, pos_tag);
      break;
    // TODO(enricozb): dup-ref optimization
    case REF:
      interact_dupref(tm, neg_loc, pos_loc);
      break;
    // case REF: link_terms(tm, neg, expand_ref(pos_loc)); break;
    case SUP:
      interact_dupsup(tm, neg_loc, pos_loc);
      break;
    default:
      //processed = false;
      break;
    }
    break;
  case MAT:
    switch (pos_tag) {
    case NUL:
      interact_matnul(tm, neg_loc, term_lab(neg));
      break;
    case U32:
    case I32:
    case F32:
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
      //processed = false;
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
    case U32:
    case REF:
    default:
      //processed = false;
      break;
    }
    break;
  default:
    //processed = false;
    break;
  }
  tm->itrs += 1;

  #if defined(DEBUG)
  if (!processed) {
    unprocessed_itrs[neg_tag * 16 + pos_tag] = 1;
    return false;
  }
  #endif

  return true;
}

#ifdef DEBUG
static void show_unprocessed_itrs() {
  for (u32 i = 17; i <= 255; i++) {
    u32 pos_tag = i % 16;
    if (pos_tag < 1) continue;
    u32 neg_tag = i / 16;
    bool hdr = true;
    if (unprocessed_itrs[neg_tag*16 + pos_tag]) {
      if (hdr) {
        fprintf(stderr, "Unprocesed itrs:\n");
        hdr = false;
      }
      fprintf(stderr, "%s%s\n", tag_to_str(neg_tag), tag_to_str(pos_tag));
    }
  }
}
#endif

static bool sequential_step(TM* tm) {
  Loc loc = redex_pop_loc(tm);
  if (loc == 0) {
    return false;
  }
  Pair pair = take_pair(loc);
  interact(tm, pair_neg(pair), pair_pos(pair));
  return true;
}
 
// don't allow idling if our booty bag isn't held
// because it introduces weirdness that's easier to just ignore for now
 __attribute__((unused))
static bool can_idle(TM *tm) {
  return rbag_empty(tm) && bbag_empty(tm) && tm->bhld && oflw_empty(tm);
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
  return (tm->tid - 1) % TPC;
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

      #if 0 || defined(DEBUG)
      if (bty_debug) {
        fprintf(stderr, "%u stealing from own held bbag!\n", tm->tid);
      }
      #endif
      return true;
    } else {
      if (bbag_pickup(tm)) {
        return true;
      }
    }
  }
  // Our booty bag is either empty, or another thread stole it - try to steal
  // another thread's full bag
  if (bbag_steal(tm, get_victim(tm))) {
    tm->sgud += 1;
    return true;
  }
  tm->sbad += 1;
  return false;
}

#define IDLE 256

static bool timeout(u64 tick) {
  if (tick % IDLE == 0) {
    u32 idle = atomic_load_explicit(&net.idle, memory_order_relaxed);
    if (idle == TPC) {
      return true;
    }
  }
  return false;
}

static bool lala_and_interact(TM *tm, Pair pair) {
  if ((tm->sid < TPC) && (tm->spop == 0)) {
    // The booty bag we stole just became empty
    if (tm->sid != tm->tid) {
      // It was another threads bag - toss it
      bbag_toss(tm);
    } else {
      #if 0 || defined(DEBUG)
      if (bty_debug) {
        fprintf(stderr, "%u emptied own stolen booty bag\n", tm->tid);
      }
      #endif
    }
    // No longer stealing
    tm->sid = TPC;
  }

  return interact(tm, pair_neg(pair), pair_pos(pair));
}

static void* thread_func(void* arg) {
  thread_id = (u64)arg;
  TM *tm = tms[thread_id];

  // Wait until after injection to turn these on
  tm->buse = true;
  tm->ouse = true;
  //u64 do_nothing = 0;

  u64  tick = 0;
  bool busy = tm->tid == 0;
  while (true) {
    tick += 1;

    // Try to recover stolen and emptied booty bag
    if (!tm->bhld) {
      bbag_recover(tm);
    }

    Loc loc = redex_pop_loc(tm);
    if (loc) {
      busy = set_busy(busy);
      
      Pair pair = take_pair(loc);
      lala_and_interact(tm, pair);

      #if 1
      if (tm->bhld && (tm->sid == TPC) && bbag_full(tm)) {
        // We're holding a full, non-stolen booty bag. Consider dropping it.
        if (!rbag_empty(tm) || !oflw_empty(tm)) {
          bbag_drop(tm);
        }
      }
      #endif
    } else {
      //do_nothing += 1;
      if (busy && can_idle(tm)) {
        busy = set_idle(busy);
      }
      if (!try_steal(tm) && !busy) {
        sched_yield();
        if (timeout(tick))
          break;
      }
    }
  }

  //atomic_fetch_add(&net.nods, tm->nput);
  //atomic_fetch_add(&net.itrs, tm->itrs);

  #ifdef SUMMARY
  fprintf(stderr, "t%u itrs %" PRIu64 ", steals good %u bad %u oflw %" PRIu64 "\n"); // do_nothing %" PRIu64 "\n",
          tm->tid, tm->itrs, tm->sgud, tm->sbad, noflw);//, do_nothing);
  #endif
  return NULL;
}

static void parallel_normalize() {
  atomic_store_explicit(&net.idle, TPC-1, memory_order_relaxed);

  for (u64 i = 0; i < TPC; i++) {
    pthread_create(&threads[i], NULL, thread_func, (void*)i);
  }
  for (u64 i = 0; i < TPC; i++) {
    pthread_join(threads[i], NULL);
  }

  #ifdef DEBUG
  show_unprocessed_itrs();
  #endif
}

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
    net.nods = tm->nput;
    net.itrs = tm->itrs;
  } else {
    parallel_normalize();
  }

  return get(0);
}

void handle_failure() {
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
  case U32:
    return "U32";
  case I32:
    return "I32";
  case F32:
    return "F32";
  case MAT:
    return "MAT";

  default:
    return "???";
  }
}

static void dump_term(Loc loc) {
  Term term = get(loc);
  printf("%04u %03u %03u %s\n", loc, term_loc(term), term_lab(term),
      tag_to_str(term_tag(term)));
}

// FILE VERSION: (or you can >> the stdio into a file)
// NOTE: broken don't use without fixing.
/*void dump_buff() {*/
/*  FILE *file = fopen("multi.txt", "w");*/
/*  if (file == NULL) {*/
/*    perror("Error opening file");*/
/*    return;*/
/*  }*/
/**/
/*  fprintf(file, "------------------\n");*/
/*  fprintf(file, "      NODES\n");*/
/*  fprintf(file, "ADDR   LOC LAB TAG\n");*/
/*  fprintf(file, "------------------\n");*/
/*  for (Loc loc = RNOD_INI; loc < RNOD_END; loc++) {*/
/*    Term term = get(loc);*/
/*    Loc t_loc = term_loc(term);*/
/*    Lab t_lab = term_lab(term);*/
/*    Tag t_tag = term_tag(term);*/
/*    fprintf(file, "%06X %03X %03X %s\n", loc, term_loc(term),
 * term_lab(term),*/
/*            tag_to_str(term_tag(term)));*/
/*  }*/
/**/
/*  fprintf(file, "------------------\n");*/
/*  fprintf(file, "    REDEX BAG\n");*/
/*  fprintf(file, "ADDR   LOC LAB TAG\n");*/
/*  fprintf(file, "------------------\n");*/
/*  for (Loc loc = RBAG + RBAG_INI; loc < RBAG + RBAG_END; loc++) {*/
/*    Term term = get(loc);*/
/*    Loc t_loc = term_loc(term);*/
/*    Lab t_lab = term_lab(term);*/
/*    Tag t_tag = term_tag(term);*/
/*    fprintf(file, "%06X %03X %03X %s\n", loc, term_loc(term),
 * term_lab(term),*/
/*            tag_to_str(term_tag(term)));*/
/*  }*/
/**/
/*  fprintf(file, "------------------\n");*/
/**/
/*  fclose(file);*/
/*}*/
// STD VERSION
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
 
void dump_buff() {
  tm_dump_buff(tms[0]);
}
