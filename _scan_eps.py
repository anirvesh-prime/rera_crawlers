import re, glob

TICK = chr(96)
for f in glob.glob('/tmp/chunk-*.js'):
    s = open(f).read()
    for m in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)\(([a-zA-Z,]*)\)\{', s):
        name = m.group(1)
        body = s[m.end():m.end()+300]
        if 'http.' in body and ('APIUrl' in body or 'MvcUrl' in body):
            u = re.search(r'(APIUrl|APIUrlHome|MvcUrl)[^,;)]{0,80}', body)
            verb = 'post' if '.post(' in body else 'get'
            ep = u.group(0) if u else '??'
            ep = ep.replace(TICK, '')
            print(f"{f.split('/')[-1]:22} {verb:4} {name}({m.group(2)}) -> {ep[:70]}")
