#!/usr/bin/env python3
"""Summarise a samply profile (recorded with --save-only --unstable-presymbolicate):
top functions by self and inclusive samples, using the .syms.json sidecar for names.

usage: profile_top.py profile.json [N] [thread-name-substring]
"""
import bisect, json, sys
from collections import Counter

def load_syms(path):
    try:
        s = json.load(open(path.replace('.json', '.syms.json')))
    except FileNotFoundError:
        return None
    strings = s['string_table']
    libs = {}
    for lib in s['data']:
        table = sorted(lib['symbol_table'], key=lambda e: e['rva'])
        starts = [e['rva'] for e in table]
        known = {a: strings[i] for a, i in lib['known_addresses']}
        libs[lib['debug_name']] = (starts, table, known, strings)
    return libs

def symbolicate(libs, libname, rva):
    if not libs or libname not in libs:
        return None
    starts, table, known, strings = libs[libname]
    if rva in known:
        return known[rva]
    i = bisect.bisect_right(starts, rva) - 1
    if i >= 0 and rva < table[i]['rva'] + max(table[i]['size'], 1):
        return strings[table[i]['symbol']]
    return None

def main(path, n=30, thread_filter=None):
    p = json.load(open(path))
    libs = load_syms(path)
    self_c, total_c = Counter(), Counter()
    nsamples = 0
    for t in p['threads']:
        if thread_filter and thread_filter not in t.get('name', ''):
            continue
        strings = t.get('stringArray') or p.get('shared', {}).get('stringArray', [])
        st, ft, fn, rt = t['stackTable'], t['frameTable'], t['funcTable'], t['resourceTable']
        libidx = {i: p['libs'][l]['name'] for i, l in enumerate(rt.get('lib', []))}
        cache = {}
        def fname(stack):
            fr = st['frame'][stack]
            if fr in cache:
                return cache[fr]
            func = ft['func'][fr]
            name = strings[fn['name'][func]]
            res = fn['resource'][func]
            lib = libidx.get(res)
            addr = ft['address'][fr]
            sym = symbolicate(libs, lib, addr) if addr is not None else None
            out = sym or (f"{lib}!{name}" if lib else name)
            cache[fr] = out
            return out
        samples = t['samples']
        stacks = samples['stack'] if 'stack' in samples else samples['data']
        for s in stacks:
            if s is None:
                continue
            nsamples += 1
            self_c[fname(s)] += 1
            seen, cur = set(), s
            while cur is not None:
                nm = fname(cur)
                if nm not in seen:
                    seen.add(nm); total_c[nm] += 1
                cur = st['prefix'][cur]
    print(f"samples: {nsamples}")
    print(f"\n{'self%':>6} {'incl%':>6}  function")
    for name, c in self_c.most_common(n):
        print(f"{100*c/nsamples:6.1f} {100*total_c[name]/nsamples:6.1f}  {name[:120]}")

if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 30, sys.argv[3] if len(sys.argv) > 3 else None)
