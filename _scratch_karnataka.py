import json
from bs4 import BeautifulSoup
from sites import karnataka_rera as k

html = open('/tmp/ka_detail.html').read()
soup = BeautifulSoup(html, 'lxml')

print('===== GRID KV (label -> value) containing promoter/contact/email/mobile/website/district/taluk/pin =====')
grid = k._extract_grid_kv(soup)
for label, val in grid.items():
    if any(w in label for w in ('promoter', 'email', 'mail', 'mobile', 'phone',
                                 'contact', 'website', 'district', 'taluk', 'pin',
                                 'village', 'address', 'latitude', 'longitude')):
        print(f'  {label!r:50} -> {val!r}')

print()
print('===== CRAWLER _parse_detail OUTPUT (relevant fields) =====')
ack = 'ACK/KA/RERA/1248/469/PR/110223/006823'
out = k._parse_detail(html, ack, 'Ballari', 0, meta={})
for f in ('promoter_name', 'promoter_contact_details', 'promoter_address_raw',
          'professional_information', 'project_city', 'project_location_raw',
          'project_pin_code', 'promoters_details'):
    print(f'  {f} = {json.dumps(out.get(f), ensure_ascii=False)}')
