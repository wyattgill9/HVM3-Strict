#ifndef RUSTY_C_H
#define RUSTY_C_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef _Atomic(u8) a8;
typedef _Atomic(u16) a16;
typedef _Atomic(u32) a32;
typedef _Atomic(u64) a64;

typedef unsigned __int128 u128 __attribute__((aligned(16)));

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef float f32;
typedef double f64;

typedef size_t usize;
typedef ptrdiff_t isize;

// panic macro
#define panic(msg) do { \
    fprintf(stderr, "panic: %s at %s:%d\n", msg, __FILE__, __LINE__); \
    abort(); \
} while(0)

// assert-like macro
#define expect(cond, msg) do { \
    if (!(cond)) panic(msg); \
} while(0)

// string slice
typedef struct {
    const char *ptr;
    usize len;
} str_slice;

static inline str_slice str_from_cstr(const char *s) {
    return (str_slice){ .ptr = s, .len = strlen(s) };
}

static inline str_slice str_from_parts(const char *p, usize l) {
    return (str_slice){ .ptr = p, .len = l };
}

// owned string
typedef struct {
    char *data;
    usize len;
    usize cap;
} String;

static inline String string_new(void) {
    return (String){ .data = NULL, .len = 0, .cap = 0 };
}

static inline String string_from_cstr(const char *s) {
    usize len = strlen(s);
    String str = { .data = malloc(len + 1), .len = len, .cap = len + 1 };
    if (!str.data) panic("allocation failed");
    memcpy(str.data, s, len + 1);
    return str;
}

static inline void string_free(String *s) {
    free(s->data);
    s->data = NULL;
    s->len = 0;
    s->cap = 0;
}

// math
static inline u8 max_u8(u8 a, u8 b)   { return a > b ? a : b; }
static inline u16 max_u16(u16 a, u16 b) { return a > b ? a : b; }
static inline u32 max_u32(u32 a, u32 b) { return a > b ? a : b; }
static inline u64 max_u64(u64 a, u64 b) { return a > b ? a : b; }

static inline i8 max_i8(i8 a, i8 b)   { return a > b ? a : b; }
static inline i16 max_i16(i16 a, i16 b) { return a > b ? a : b; }
static inline i32 max_i32(i32 a, i32 b) { return a > b ? a : b; }
static inline i64 max_i64(i64 a, i64 b) { return a > b ? a : b; }

static inline u8 min_u8(u8 a, u8 b)   { return a < b ? a : b; }
static inline u16 min_u16(u16 a, u16 b) { return a < b ? a : b; }
static inline u32 min_u32(u32 a, u32 b) { return a < b ? a : b; }
static inline u64 min_u64(u64 a, u64 b) { return a < b ? a : b; }

static inline i8 min_i8(i8 a, i8 b)   { return a < b ? a : b; }
static inline i16 min_i16(i16 a, i16 b) { return a < b ? a : b; }
static inline i32 min_i32(i32 a, i32 b) { return a < b ? a : b; }
static inline i64 min_i64(i64 a, i64 b) { return a < b ? a : b; }

static inline f32 max_f32(f32 a, f32 b) { return a > b ? a : b; }
static inline f32 min_f32(f32 a, f32 b) { return a < b ? a : b; }
static inline f64 max_f64(f64 a, f64 b) { return a > b ? a : b; }
static inline f64 min_f64(f64 a, f64 b) { return a < b ? a : b; }

// Clamp functions
static inline u8 clamp_u8(u8 v, u8 lo, u8 hi) { return max_u8(lo, min_u8(v, hi)); }
static inline u16 clamp_u16(u16 v, u16 lo, u16 hi) { return max_u16(lo, min_u16(v, hi)); }
static inline u32 clamp_u32(u32 v, u32 lo, u32 hi) { return max_u32(lo, min_u32(v, hi)); }
static inline u64 clamp_u64(u64 v, u64 lo, u64 hi) { return max_u64(lo, min_u64(v, hi)); }

static inline i8 clamp_i8(i8 v, i8 lo, i8 hi) { return max_i8(lo, min_i8(v, hi)); }
static inline i16 clamp_i16(i16 v, i16 lo, i16 hi) { return max_i16(lo, min_i16(v, hi)); }
static inline i32 clamp_i32(i32 v, i32 lo, i32 hi) { return max_i32(lo, min_i32(v, hi)); }
static inline i64 clamp_i64(i64 v, i64 lo, i64 hi) { return max_i64(lo, min_i64(v, hi)); }

static inline f32 clamp_f32(f32 v, f32 lo, f32 hi) { return max_f32(lo, min_f32(v, hi)); }
static inline f64 clamp_f64(f64 v, f64 lo, f64 hi) { return max_f64(lo, min_f64(v, hi)); }

// Abs functions
static inline i8 abs_i8(i8 v) { return v < 0 ? -v : v; }
static inline i16 abs_i16(i16 v) { return v < 0 ? -v : v; }
static inline i32 abs_i32(i32 v) { return v < 0 ? -v : v; }
static inline i64 abs_i64(i64 v) { return v < 0 ? -v : v; }
static inline f32 abs_f32(f32 v) { return v < 0.0f ? -v : v; }
static inline f64 abs_f64(f64 v) { return v < 0.0 ? -v : v; }

// swap functions
static inline void swap_u8(u8 *a, u8 *b) { u8 t = *a; *a = *b; *b = t; }
static inline void swap_u16(u16 *a, u16 *b) { u16 t = *a; *a = *b; *b = t; }
static inline void swap_u32(u32 *a, u32 *b) { u32 t = *a; *a = *b; *b = t; }
static inline void swap_u64(u64 *a, u64 *b) { u64 t = *a; *a = *b; *b = t; }

static inline void swap_i8(i8 *a, i8 *b) { i8 t = *a; *a = *b; *b = t; }
static inline void swap_i16(i16 *a, i16 *b) { i16 t = *a; *a = *b; *b = t; }
static inline void swap_i32(i32 *a, i32 *b) { i32 t = *a; *a = *b; *b = t; }
static inline void swap_i64(i64 *a, i64 *b) { i64 t = *a; *a = *b; *b = t; }

static inline void swap_f32(f32 *a, f32 *b) { f32 t = *a; *a = *b; *b = t; }
static inline void swap_f64(f64 *a, f64 *b) { f64 t = *a; *a = *b; *b = t; }

#endif // RUSTY_C_H
