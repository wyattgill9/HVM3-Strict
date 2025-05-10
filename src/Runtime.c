// HVM3-Strict Core: single-thread, polarized, LAM/APP & DUP/SUP only
// parallel support in progress.

#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DEBUG_LOG(fmt, ...) fprintf(stderr, "[DEBUG] " fmt "\n", ##__VA_ARGS__)

typedef pthread_t thread_t;

int get_num_threads() {
  long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  return (nprocs <= 1) ? 1 : 1 << (int)log2(nprocs);
}

int thread_create(pthread_t *thread, const pthread_attr_t *attr,
                  void *(*start_routine)(void *), void *arg) {
  return pthread_create(thread, attr, start_routine, arg);
}

int thread_join(pthread_t thread, void **retval) {
  return pthread_join(thread, retval);
}

//const int TPC = get_num_threads(); // don't enable this, change the enum below

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

// Types
typedef int32_t i32;
typedef uint32_t u32;
typedef _Atomic(u32) a32;
typedef float f32;
typedef int64_t i64;
typedef uint64_t u64;
typedef _Atomic(u64) a64;
// NOTE alignment is gcc/clang specific
typedef unsigned __int128 u128 __attribute__((aligned(16)));

typedef uint8_t Tag; //  8 bits
typedef u32 Lab;     // 24 bits
typedef u32 Loc;     // 32 bits
typedef u64 Term;    // Loc | Lab | Tag
typedef u128 Pair;   // Term | Term

// Using union for type punning (safer in C)
typedef union {
  u32 u;
  i32 i;
  f32 f;
} TypeConverter;

// Global heap
static u64 *BUFF = NULL;  // NOTE: intentionally *not* atomic ptr.

// Heap configuration options
enum : u64 {
  // 1GiB heap
  HEAP_1GB = (1ULL << 27) * sizeof(u64),  // 128Mi * 8 bytes = 1GiB

  //////////////////////////
  // Choose a heap size here
  //////////////////////////
  HEAP_SIZE = HEAP_1GB * 4,

  // Cache line size
  CLINE_BYTES = 64,
  CLINE_U64 = CLINE_BYTES / sizeof(u64),

  // Threads per CPU
  TPC = 8,

  // Various redex bag starting indices within the heap to choose from.
  // The remaining percentage is used for node storage.

  // Cache-aligned and named for the percentage of heap used.
  RBAG_25_PCT = (HEAP_SIZE / 4),
  RBAG_50_PCT = (HEAP_SIZE / 2),
  RBAG_75_PCT = (HEAP_SIZE - (HEAP_SIZE / 4)),
  // TODO: "RBAG_16M" to match HVM2

  ////////////////////////////////////
  // Choose a RBAG size here
  ////////////////////////////////////
  RBAG_SIZE = RBAG_25_PCT
};

enum : u32 {
  // Final calculated RBAG index
  RBAG = (HEAP_SIZE - RBAG_SIZE) / sizeof(u64),

  // Calculate RBAG_LEN and NODE_LEN: number of u64 elements *per thread*
  // TODO: MUST be 16-byte aligned. 64-byte alignment might be preferable.
  // TODO: actually these could be in terms of u128 elements
  RBAG_LEN = RBAG_SIZE / (TPC * sizeof(u64)),
  NODE_LEN = (HEAP_SIZE - RBAG_SIZE) / (TPC * sizeof(u64))
};

typedef struct Net {
  a64 itrs;
  a32 idle;
} Net;

// Global book
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

static Net net = {
  .itrs = 0,
  .idle = 0
};

static Book BOOK = {
  .defs = NULL,
  .len = 0,
  .cap = 0,
};

const Term VOID = 0;

// Local Thread Memory
typedef struct TM {
  u32  tid; // thread id
  Loc  nput; // next node allocation attempt index
  Loc  rput; // next rbag push index
  //u32  hput; // next hbag push index
  Loc  sidx; // steal index
  u64  itrs; // interaction count
  Loc  nloc[0xFFF]; // node allocation indices
  //Pair hbag_buf[HLEN]; // high-priority redexes
} TM;

static TM *tms[TPC];

// Debugging
static char *tag_to_str(Tag tag);
static char *itr_str(u32 itr);
void dump_term(Loc loc);
void dump_buff(TM *tm);

// TM operations
void tm_reset(TM *tm) {
  tm->nput = 0;
  tm->rput = 0;
  //tm->hput = 0;
  tm->sidx = 0;
  tm->itrs = 0;
}

TM *tm_new(u64 tid) {
  TM *tm = malloc(sizeof(TM));
  if (tm == NULL) {
    fprintf(stderr, "TM memory allocation failed\n");
    exit(EXIT_FAILURE);
  }
  tm->tid = tid;
  tm_reset(tm);
  return tm;
}

void alloc_static_tms() {
  for (u64 t = 0; t < TPC; ++t) {
    tms[t] = tm_new(t);
  }
}

void free_static_tms() {
  for (u64 t = 0; t < TPC; ++t) {
    if (tms[t] != NULL) {
      free(tms[t]);
      tms[t] = NULL;
    }
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

Tag term_tag(Term term) { return term & 0xFF; }

Lab term_lab(Term term) { return (term >> 8) & 0xFFFFFF; }

Loc term_loc(Term term) { return (term >> 32) & 0xFFFFFFFF; }

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

// This should only be called with BOOK Def nodes, which are always contiguous
// and always have locs relative to the beginning of Def.nodes.
static Term term_with_nloc(TM *tm, Term term, u32 nodes_len) {
  if (!term_has_loc(term)) { return term; }
  Loc loc = term_loc(term);
  // TODO: Temporary sanity check.
  #if 1 || defined(DEBUG)
  if (/*loc < 1 || */loc > nodes_len) {
    fprintf(stderr, "Invalid loc %u, nodes %u\n", loc, nodes_len);
    exit(1);
  }
  #endif
  return term_with_loc(term, tm->nloc[loc]);
}

static int mop_debug = 0; // memory operations
static int thd_debug = 0; // threading

static _Thread_local int thread_id = 0;

#define TERMSTR_BUFSIZ 128

static const char* term_str(char* buf, Term term) {
  sprintf(buf, "%s lab:%u loc:%u", tag_to_str(term_tag(term)),
          (int)term_lab(term), (int)term_loc(term));
  return buf;
}

static bool good_loc(Loc loc) { return true; }

// Memory operations
Term swap(Loc loc, Term term) {
  Term res = atomic_exchange_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);

  if (mop_debug && good_loc(loc)) {
    char buf1[TERMSTR_BUFSIZ];
    char buf2[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d swap %u %s with %s\n", thread_id,
        /*itr_str(itr),*/
        loc, term_str(buf1, res), term_str(buf2, term));
  }

  return res;
}

Term get(Loc loc) {
  Term term = atomic_load_explicit((a64*)&BUFF[loc], memory_order_relaxed);

  if (mop_debug && good_loc(loc)) {
    char buf[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d get %u %s\n", thread_id, loc, term_str(buf, term));
  }

  return term;
}

Term take(Loc loc) {
  Term term = atomic_exchange_explicit((a64*)&BUFF[loc], VOID, memory_order_relaxed);

  //int invalid = ((term_tag(term) == VOID) ? 1 : 0);
  if (/*invalid ||*/ (mop_debug && good_loc(loc))) {
    char buf[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d take %u %s\n", thread_id, loc, term_str(buf, term));
  }
  /*
  if (invalid) {
    fprintf(stderr, "VOID term taken.\n");
    exit(1);
  }
  */

  return term;
}

void set(Loc loc, Term term) {
  atomic_store_explicit((a64*)&BUFF[loc], term, memory_order_relaxed);

  if (mop_debug && good_loc(loc)) {
    char buf[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d set %u %s\n", thread_id, loc, term_str(buf, term));
  }
}

static Pair take_pair(Loc loc) {
  Pair pair = __atomic_exchange_n((Pair*)&BUFF[loc], 0ULL, __ATOMIC_RELAXED);

  if (mop_debug) {
    char buf[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d take.neg %u %s\n", thread_id, loc, term_str(buf, pair_neg(pair)));
    fprintf(stderr, "%d take.pos %u %s\n", thread_id, loc + 1, term_str(buf, pair_pos(pair)));
  }

  return pair;
}

static void set_pair(Loc loc, Pair pair) {
  __atomic_store_n((Pair*)&BUFF[loc], pair, __ATOMIC_RELAXED);

  if (mop_debug) {
    char buf[TERMSTR_BUFSIZ];
    fprintf(stderr, "%d set.neg %u %s\n", thread_id, loc, term_str(buf, pair_neg(pair)));
    fprintf(stderr, "%d set.pos %u %s\n", thread_id, loc + 1, term_str(buf, pair_pos(pair)));
  }
}

static Loc port(u64 n, Loc loc) { return n + loc - 1; }

// Allocator
// ---------

/*
static u32 node_alloc_1(TM *tm) {
  u32 att = 0; // attempts
  while (1) {
    // TODO: consider nput + npos, with npos resetting to 1, to eliminate modulo
    Loc loc = tm->tid * NODE_LEN + (tm->nput % NODE_LEN);
    Pair pair = *(Pair*)&BUFF[loc];
    tm->nput += 2;
    if (loc > 0 && pair == 0) { return loc; }
    if (++att >= NODE_LEN / 2) {
      fprintf(stderr, "Thread %u node space exhausted\n", tm->tid);
      exit(EXIT_FAILURE);
    }
  }
}
*/

static u32 node_alloc(TM *tm, u32 num) {
  if (mop_debug) {
    fprintf(stderr, "%u alloc %u nodes at %u\n", tm->tid, num, tm->nput);
  }
  u32 got = 0;
  u32 att = 0; // attempts
  while (got < num) {
    Loc loc = tm->tid * NODE_LEN + (tm->nput % NODE_LEN);
    Pair pair = *(Pair*)&BUFF[loc];
    tm->nput += 1;
    if (loc > 0 && pair == 0) {
      tm->nloc[got++] = loc;
    }
    #ifndef RECYCLE_NODES
    else {
      // This disables node recycling due to contiguous node requirement
      // in interact_mat*.
      fprintf(stderr, "Node space exhausted or non-zero node-pair found, %u\n",
          loc);
      exit(EXIT_FAILURE);
    }
    #else
    // TODO: potential to not exit here and wait for var nodes to be zero'd
    // and become available, but simplifying by treating as fatal for now.
    if (++att >= NODE_LEN / 2) {
      fprintf(stderr, "Thread %u node space exhausted (%u)\n", tm->tid, num);
      exit(EXIT_FAILURE);
    }
    #endif
  }
  return got;
}

// Gets the necessary resources for an interaction. Returns success.
static inline bool get_resources(TM *tm, u32 need_rbag, u32 need_node) {
  // with no HBAG implemented, treat RBAG exhaustion as fatal
  u32 got_rbag = RBAG_LEN - tm->rput;
  if (got_rbag < need_rbag) {
    fprintf(stderr, "Thread %u redex space exhausted\n", tm->tid);
    exit(EXIT_FAILURE);
  }
  u32 got_node = node_alloc(tm, need_node);
  return got_node >= need_node;
}

// TODO: rbag_push_pair

static void rbag_push(TM *tm, Term neg, Term pos) {
  #if 1 || defined(DEBUG)
  //bool free_local = tm->hput < HLEN;
  bool free_global = tm->rput < RBAG_LEN - 1;
  if (!free_global /* || !free_local*/) {
    fprintf(stderr, "rbag space exhausted\n");
    exit(1);
  }
  #endif

  /*
  if (is_high_priority(get_pair_rule(redex))) {
    tm->hbag_buf[tm->hput++] = redex;
  } else {
  */
  Loc loc = RBAG + tm->tid * RBAG_LEN + tm->rput;

  if (0 && mop_debug) {
    fprintf(stderr, "%u calling set_pair @ %u, rput %u\n", tm->tid, loc, tm->rput);
  }

  set_pair(loc, pair_new(neg, pos));
  tm->rput += 2;
  //}
}

static Pair rbag_pop(TM* tm) {
  /*if (tm->hput > 0) {
    return tm->hbag_buf[--tm->hput];
  } else */
  if (tm->rput > 0) {
    tm->rput -= 2;
    return RBAG + tm->tid * RBAG_LEN + tm->rput;
  } else {
    return 0;
  }
}

// FFI functions
void hvm_init() {
  if (BUFF == NULL) {
    BUFF = aligned_alloc(CLINE_BYTES, HEAP_SIZE);
    if (BUFF == NULL) {
      fprintf(stderr, "Heap memory allocation failed\n");
      exit(EXIT_FAILURE);
    }
  }
  memset(BUFF, 0, HEAP_SIZE);

  alloc_static_tms();

  fprintf(stderr, "HEAP_SIZE = %llu\n", HEAP_SIZE);
  fprintf(stderr, "RBAG_SIZE = %llu\n", RBAG_SIZE);
  fprintf(stderr, "RBAG      = %u\n", RBAG);
  fprintf(stderr, "RBAG_LEN  = %u\n", RBAG_LEN);
  fprintf(stderr, "NODE_LEN  = %u\n", NODE_LEN);
}

void hvm_free() {
  if (BUFF != NULL) {
    free(BUFF);
    BUFF = NULL;
  }
  free_static_tms();
}

Loc ffi_alloc_node(u64 arity) {
  // Only called by Inject.hs. This is a special case of node allocation
  // where we know nodes will be contiguous.
  TM *tm = tms[0];
  Loc loc = tm->nput;
  tm->nput += arity;
  return loc;
}

void ffi_rbag_push(Term neg, Term pos) {
  rbag_push(tms[0], neg, pos);
}

u64 inc_itr() {
  return net.itrs;
} 

Loc rbag_ini() {
  // TODO
  return RBAG;
}

Loc rbag_end() {
  // TODO
  return RBAG;
}

Loc rnod_end() {
  TM *tm = tms[0];
  return tm->nput;
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
  Loc rbag_end = tm->rput; // atomic_load(&RBAG_END);
  Loc rnod_end = tm->nput; // atomic_load(&RNOD_END);

  Def def = {
      .name = name,
      .nodes = calloc(rnod_end, sizeof(Term)),
      .nodes_len = rnod_end,
      .rbag = calloc(rbag_end, sizeof(Term)),
      .rbag_len = rbag_end,
  };

  memcpy(def.nodes, BUFF, sizeof(Term) * def.nodes_len);
  memcpy(def.rbag, BUFF + RBAG, sizeof(Term) * def.rbag_len);

  // printf("NEW DEF '%s':\n", def.name);
  // dump_buff();
  // printf("\n");

  memset(BUFF, 0, sizeof(Term) * def.nodes_len);
  memset(BUFF + RBAG, 0, sizeof(Term) * def.rbag_len);

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

  // offset calculation must occur before get_resources() call
  //Loc offset = atomic_fetch_add(&RNOD_END, nodes_len - 1) - 1;
  Loc offset = (tm->tid * NODE_LEN) + tm->nput - 1;
  //if (tm->tid == 0) offset -= 1;

  if (mop_debug) {
    fprintf(stderr, "%u expand_ref %u nodes %u rbag %u offset %u\n", tm->tid,
        def_idx, nodes_len, rbag_len, offset);
  }

  // TODO: If this fails, the Ref term could be pushed back onto bag.
  // For simplicity, exit for now.
  if (!get_resources(tm, rbag_len, nodes_len - 1)) {
    fprintf(stderr, "Thread %u node space exhausted, nodes %u\n",
           tm->tid, nodes_len);
    exit(1);
  }

  #ifdef RECYCLE_NODES
  // All nodes in a book def are contiguous and have zero-based locs, relative
  // to the def's node array.
  //
  // Conversely, the nodes of an expanded ref may be dispersed, resulting in no
  // fixed offset with which we can modify the locs of expanded terms.
  //
  // Instead, we must (potentially) set the loc of every expanded term to the
  // relatively positioned loc reserved by get_resources() above, in tm->nloc.
  Term root = term_with_nloc(tm, nodes[0], nodes_len);
  #else
  Term root = term_offset_loc(nodes[0], offset);
  #endif

  // No redexes reference these nodes yet; therefore, safe to add un-atomically.
  for (u32 i = 1; i < nodes_len; i++) {
    #ifdef RECYCLE_NODES
    Loc loc = tm->nloc[i - 1];
    Term term = term_with_nloc(tm, nodes[i], nodes_len);
    #else
    Loc loc = offset + i;
    Term term = term_offset_loc(nodes[i], offset);
    #endif
    BUFF[loc] = term;
    if (mop_debug) {
      char buf[TERMSTR_BUFSIZ];
      fprintf(stderr, "%u node %u %s\n", tm->tid, loc, term_str(buf, term));
    }
  }

  for (u32 i = 0; i < rbag_len; i += 2) {
    #ifdef RECYCLE_NODES
    Term neg = term_with_nloc(tm, rbag[i], nodes_len);
    Term pos = term_with_nloc(tm, rbag[i + 1], nodes_len);
    #else
    Term neg = term_offset_loc(rbag[i], offset);
    Term pos = term_offset_loc(rbag[i + 1], offset);
    #endif
    rbag_push(tm, neg, pos);
  }
  return root;
}

static void boot(Loc def_idx) {
  TM *tm = tms[0];
  if (tm->nput > 0 || tm->rput > 0) {
    fprintf(stderr, "booting on non-empty state\n");
    exit(1);
  }
  //fprintf(stderr, "booting def %u\n", def_idx);
  tm->nput = 1; // node_alloc_1, effectively
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
    rbag_push(tm, neg, pos);
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
static void interact_applam(TM *tm, Loc a_loc, Loc b_loc) {
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Loc var = port(1, b_loc);
  Term bod = take(port(2, b_loc));
  move(tm, var, arg);
  move(tm, ret, bod);
}

static void interact_appsup(TM *tm, Loc a_loc, Loc b_loc) {
  // TODO: recoverable
  if (!get_resources(tm, 0, 8)) {
    fprintf(stderr, "i_appsup: Thread %u node space exhausted\n", tm->tid);
    exit(1);
  }
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = tm->nloc[0];
  Loc dp2 = tm->nloc[2];
  Loc cn1 = tm->nloc[4];
  Loc cn2 = tm->nloc[6];
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

static void interact_opxnum(TM *tm, Loc a_loc, Lab op, u32 num, Tag num_type) {
  Term arg = swap(port(1, a_loc), term_new(num_type, 0, num));
  link_terms(tm, term_new(OPY, op, a_loc), arg);
}

static void interact_opxsup(TM *tm, Loc a_loc, Lab op, Loc b_loc) {
  // TODO: recoverable
  if (!get_resources(tm, 0, 8)) {
    fprintf(stderr, "i_opxsup: Thread %u node space exhausted\n", tm->tid);
    exit(1);
  }
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = tm->nloc[0];
  Loc dp2 = tm->nloc[2];
  Loc cn1 = tm->nloc[4];
  Loc cn2 = tm->nloc[6];
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
  // TODO: recoverable
  if (!get_resources(tm, 0, 8)) {
    fprintf(stderr, "i_appsup: Thread %u node space exhausted\n", tm->tid);
    exit(1);
  }
  Term arg = take(port(1, a_loc));
  Loc ret = port(2, a_loc);
  Term tm1 = take(port(1, b_loc));
  Term tm2 = take(port(2, b_loc));
  Loc dp1 = tm->nloc[0];
  Loc dp2 = tm->nloc[2];
  Loc cn1 = tm->nloc[4];
  Loc cn2 = tm->nloc[6];
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
  // TODO: recoverable
  if (!get_resources(tm, 0, 8)) {
    fprintf(stderr, "i_appsup: Thread %u node space exhausted\n", tm->tid);
    exit(1);
  }
  Loc dp1 = port(1, a_loc);
  Loc dp2 = port(2, a_loc);
  Loc var = port(1, b_loc);
  // TODO(enricozb): why is this the only take?
  Term bod = take(port(2, b_loc));
  Loc co1 = tm->nloc[0];
  Loc co2 = tm->nloc[2];
  Loc du1 = tm->nloc[4];
  Loc du2 = tm->nloc[6];
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
/*
  move(tm, port(1, a_loc), term_new(NUL, 0, 0));
  for (u32 i = 0; i < mat_len; i++) {
    // TODO: problematic port() usage
    link_terms(tm, term_new(ERA, 0, 0), take(port(i + 2, a_loc)));
  }
*/
}

static void interact_matnum(TM *tm, Loc mat_loc, Lab mat_len, u32 n, Tag n_type) {
  if (n_type != U32) {
    fprintf(stderr, "match with non-U32\n");
    exit(1);
  }

  u32 i_arm = (n < mat_len - 1) ? n : (mat_len - 1);
  for (u32 i = 0; i < mat_len; i++) {
    if (i != i_arm) {
      // TODO: problematic port() usage
      link_terms(tm, term_new(ERA, 0, 0), take(port(2 + i, mat_loc)));
    }
  }

  Loc ret = port(1, mat_loc);
  // TODO: problematic port() usage
  Term arm = take(port(2 + i_arm, mat_loc));

  if (i_arm < mat_len - 1) {
    move(tm, ret, arm);
  } else {
    // REVIEW: using get_resources() here instead of node_alloc_1() because upon
    // review there may be a minimum # of redex spots required as well.
    // TODO: recoverable
    if (!get_resources(tm, 0, 2)) {
      fprintf(stderr, "i_matnum: Thread %u node space exhausted\n", tm->tid);
      exit(1);
    }
    Loc app = tm->nloc[0];
    set(app + 0, term_new(U32, 0, n - (mat_len - 1)));
    set(app + 1, term_new(SUB, 0, 0));
    move(tm, ret, term_new(VAR, 0, port(2, app)));

    link_terms(tm, term_new(APP, 0, app), arm);
  }
}

static void interact_matsup(TM *tm, Loc mat_loc, Lab mat_len, Loc sup_loc) {
  fprintf(stderr, "interact_matsup not supported (yet)\n");
  exit(1);
/*
  // TODO: recoverable
  if (!get_resources(tm, 0, 2 + mat_len * 3)) {
    fprintf(stderr, "i_appsup: Thread %u node space exhausted\n", tm->tid);
    exit(1);
  }
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
    // TODO: this is problematic. it requires i+mat_len nodes to be contiguous.
    // Can probably weasel around it by implementing a port_nloc() that uses
    // locs reserved by get_resources() in tm->nloc. Punting for now.
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

static void interact(TM *tm, Term neg, Term pos) {
  Tag neg_tag = term_tag(neg);
  Tag pos_tag = term_tag(pos);
  Loc neg_loc = term_loc(neg);
  Loc pos_loc = term_loc(pos);

  switch (neg_tag) {
  case APP:
    switch (pos_tag) {
    case LAM:
      interact_applam(tm, neg_loc, pos_loc); break;
    case NUL:
      interact_appnul(tm, neg_loc);
      break;
    case U32:
      interact_appu32(tm, neg_loc, pos_loc);
      break;
    case REF:
      link_terms(tm, neg, expand_ref(tm, pos_loc));
      break;
    case SUP:
      interact_appsup(tm, neg_loc, pos_loc);
      break;
    }
    break;
  case OPX:
    switch (pos_tag) {
    case LAM:
      break;
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
    }
    break;
  case OPY:
    switch (pos_tag) {
    case LAM:
      break;
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
    }
    break;
  case MAT:
    switch (pos_tag) {
    case LAM:
      break;
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
    }
    break;
  case ERA:
    switch (pos_tag) {
    case LAM:
      interact_eralam(tm, pos_loc);
      break;
    case NUL:
      break;
    case U32:
      break;
    case REF:
      break;
    case SUP:
      interact_erasup(tm, pos_loc);
      break;
    }
    break;
  }
}

static inline bool sequential_step(TM* tm) {
  Loc loc = /*sequential_*/rbag_pop(tm);

  if (loc == 0) {
    return false;
  }

  Pair pair = take_pair(loc);
  //Term neg = take(loc);
  //Term pos = take(loc + 1);
  interact(tm, pair_neg(pair), pair_pos(pair));
  return true;
}

// A simple spin-wait barrier using atomic operations
a64 a_reached = 0; // number of threads that reached the current barrier
a64 a_barrier = 0; // number of barriers passed during this program
static void sync_threads() {
  u64 barrier_old = atomic_load_explicit(&a_barrier, memory_order_relaxed);
  if (atomic_fetch_add_explicit(&a_reached, 1, memory_order_relaxed) == (TPC - 1)) {
    // Last thread to reach the barrier resets the counter and advances the barrier
    atomic_store_explicit(&a_reached, 0, memory_order_relaxed);
    atomic_store_explicit(&a_barrier, barrier_old + 1, memory_order_release);
  } else {
    u32 tries = 0;
    while (atomic_load_explicit(&a_barrier, memory_order_acquire) == barrier_old) {
      sched_yield();
    }
  }
}

static bool set_idle(bool was_busy) {
  if (was_busy) {
    u32 idle = atomic_fetch_add_explicit(&net.idle, 1, memory_order_relaxed);
    if (thd_debug) {
      fprintf(stderr, "%u set idle, was idle= %u\n", thread_id, idle);
    }

  }
  return false;
}

static bool set_busy(bool was_busy) {
  if (!was_busy) {
    u32 idle = atomic_fetch_sub_explicit(&net.idle, 1, memory_order_relaxed);
    if (thd_debug) {
      fprintf(stderr, "%u set busy, was idle = %u\n", thread_id, idle);
    }
  }
  return true;
}

/*
void evaluator(Net* net, TM* tm, Book* book) {
  // Initializes the global idle counter
  atomic_store_explicit(&net->idle, TPC - 1, memory_order_relaxed);
  sync_threads();

  // Performs some interactions
  u32  tick = 0;
  bool busy = tm->tid == 0;
  while (true) {
    tick += 1;

    // If we have redexes...
    if (rbag_len(net, tm) > 0) {
      // Update global idle counter
      if (!busy) atomic_fetch_sub_explicit(&net->idle, 1, memory_order_relaxed);
      busy = true;

      // Perform an interaction
      #ifdef DEBUG
      if (!interact(net, tm, book)) debug("interaction failed\n");
      #else
      interact(net, tm, book);
      #endif
    // If we have no redexes...
    } else {
      // Update global idle counter
      if (busy) atomic_fetch_add_explicit(&net->idle, 1, memory_order_relaxed);
      busy = false;

      //// Peeks a redex from target
      u32 sid = (tm->tid - 1) % TPC;
      u32 idx = sid*(G_RBAG_LEN/TPC) + (tm->sidx++);

      // Stealing Everything: this will steal all redexes

      Pair got = atomic_exchange_explicit(&net->rbag_buf[idx], 0, memory_order_relaxed);
      if (got != 0) {
        push_redex(net, tm, got);
        tm->sgud++;
        continue;
      } else {
        tm->sidx = 0;
        tm->sbad++;
      }

      // Chill...
      sched_yield();
      // Halt if all threads are idle
      if (tick % 256 == 0) {
        if (atomic_load_explicit(&net->idle, memory_order_relaxed) == TPC) {
          break;
        }
      }
    }
  }

  sync_threads();

  atomic_fetch_add(&net->itrs, tm->itrs);
  tm->itrs = 0;
}
*/

static u32 choose_victim(TM *tm) {
  return (tm->tid - 1) % TPC;
}

static bool try_steal(TM *tm) {
  u32 sid = choose_victim(tm);
  Loc loc = RBAG + sid * RBAG_LEN + tm->sidx;

  if (thd_debug) {
    fprintf(stderr, "%u try stealing from %u @ %u : %u\n", tm->tid, sid,
        tm->sidx, loc);
  }

  tm->sidx += 2;

  Pair got = take_pair(loc);

  if (got != 0) {
    if (thd_debug) { fprintf(stderr, "%u success, pushing...\n", tm->tid); }

    rbag_push(tm, pair_neg(got), pair_pos(got));

    if (thd_debug) { fprintf(stderr, "%u steal done\n", tm->tid); }

    // tm->sgud++;
    return true;
  } else {
    if (thd_debug) { fprintf(stderr, "%u steal failed\n", tm->tid); }

    tm->sidx = 0;
    // tm->sbad++;
    return false;
  }
}

static bool check_timeout(u32 tick) {
  if (tick % 256 == 0) {
    u32 idle = atomic_load_explicit(&net.idle, memory_order_relaxed);
    if (idle == TPC) {
     return true;
    }
    if (thd_debug) {
      fprintf(stderr, "%u idle, total: %u\n", thread_id, idle);
    }
  }
  return false;
}

static void* thread_func(void* arg) {
  thread_id = (u64)arg;
  TM *tm = tms[thread_id];

  sync_threads();
  if (thd_debug) {
    fprintf(stderr, "%u before sync done\n", tm->tid);
  }

  u32  tick = 0;
  bool busy = tm->tid == 0;
  while (true) {
    tick += 1;
    Loc loc = rbag_pop(tm);
    if (loc) {
      busy = set_busy(busy);

      Pair pair = take_pair(loc);

      interact(tm, pair_neg(pair), pair_pos(pair));
    } else {
      busy = set_idle(busy);

      //TODO! debugging
      if (/*(tm->tid > 0) && */ try_steal(tm)) continue;

      sched_yield();

      if (check_timeout(tick)) break;
    }
  }

  atomic_fetch_add(&net.itrs, tm->nput / 2);

  if (thd_debug) {
    fprintf(stderr, "%u before sync...\n", tm->tid);
  }

  sync_threads();

  if (thd_debug) {
    fprintf(stderr, "%u after sync done\n", tm->tid);
  }

  return NULL;
}

static void parallel_normalize() {
  thread_t threads[TPC];

  // Initializes the global idle counter
  atomic_store_explicit(&net.idle, TPC - 1, memory_order_relaxed);

  for (u64 i = 0; i < TPC; i++) {
    int rc = thread_create(&threads[i], NULL, thread_func, (void*)i);
  }

  for (u64 i = 0; i < TPC; i++) {
    thread_join(threads[i], NULL);
  }
}

Term normalize(Term term) {
  if (term_tag(term) != REF) {
    fprintf(stderr, "normalizing non-ref\n");
    exit(1);
  }

  boot(term_loc(term));

  /*
  if (TPC == 1) {
    TM *tm = tms[0];
    while (sequential_step(tm))
      ;
    atomic_fetch_add(&net.itrs, tm->nput / 2);
  } else {
  */
    parallel_normalize();
    //}

  return get(0);
}

// Debugging
static char *tag_to_str(Tag tag) {
  switch (tag) {
  case VOID:
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

/*
static char *itr_str(u32 itr) {
  static char buf[16];
  switch(itr) {
  case I_APPLAM: return "APPLAM";
  case I_APPNUL: return "APPNUL";
  case I_APPU32: return "APPU32";
  case I_APPREF: return "APPREF";
  case I_APPSUP: return "APPSUP";
  case I_DUPLAM: return "DUPLAM";
  case I_DUPNUL: return "DUPNUL";
  case I_DUPNUM: return "DUPNUM";
  case I_DUPREF: return "DUPREF";
  case I_DUPSUP: return "DUPSUP";
  case I_MATLAM: return "MATLAM";
  case I_MATNUL: return "MATNUL";
  case I_MATNUM: return "MATNUM";
  case I_MATREF: return "MATREF";
  case I_MATSUP: return "MATSUP";
  case I_OPXNUL: return "OPXNUL";
  case I_OPXNUM: return "OPXNUM";
  case I_OPXREF: return "OPXREF";
  case I_OPXSUP: return "OPXSUP";
  case I_OPYNUL: return "OPYNUL";
  case I_OPYNUM: return "OPYNUM";
  case I_OPYREF: return "OPYREF";
  case I_OPYSUP: return "OPYSUP";
  case I_ERALAM: return "ERALAM";
  case I_ERASUP: return "ERASUP";
  case TAKENEXT: return "_TAKE_";
  default: return "I_????";
  }
}
*/

void dump_term(Loc loc) {
  Term term = get(loc);
  printf("%06X %03X %03X %s\n", loc, term_loc(term), term_lab(term),
      tag_to_str(term_tag(term)));
}

// FILE VERSION: (or you can >> the stdio into a file)
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
void dump_buff(TM *tm) {
  printf("------------------\n");
  printf("      NODES\n");
  printf("ADDR   LOC LAB TAG\n");
  printf("------------------\n");
  for (Loc idx = 0; idx < tm->nput; idx++) {
    Loc loc = tm->tid * NODE_LEN + idx;
    dump_term(loc);
/*
    Term term = get(loc);
    printf("%06X %03X %03X %s\n", loc, term_loc(term), term_lab(term),
        tag_to_str(term_tag(term)));
*/
  }
  printf("------------------\n");
  printf("    REDEX BAG\n");
  printf("ADDR   LOC LAB TAG\n");
  printf("------------------\n");
  for (Loc idx = 0; idx < tm->rput; idx++) {
    Loc loc = RBAG + tm->tid * RBAG_LEN + idx;
    dump_term(loc);
  }
  printf("------------------\n");
}
