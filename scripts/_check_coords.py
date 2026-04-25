import pdfplumber, json

for fid in ['gaceta_1077 (2)', 'gaceta_791 (6)']:
    with open(f'output/pdf_converter_calibration/{fid}.json', encoding='utf-8') as f:
        pages = json.load(f)
    print(f'\n=== {fid} ===')
    for p in pages[:5]:
        txt = p['text'][:200].replace('\n', '|')
        print(f'  p{p["page_num"]} {p["status"]} {p["column_layout"]}: {txt}')

print('\n=== gaceta_791 (6) pages 3-6 right column words ===')
with pdfplumber.open('input/gaceta_791 (6).pdf') as pdf:
    for i in [2, 3, 4, 5]:
        page = pdf.pages[i]
        words = page.extract_words()
        right = [w for w in words if w['x0'] > 310 and w['top'] > 85]
        txt = ' '.join(w['text'] for w in right[:15])
        left = [w for w in words if w['x0'] <= 310 and w['top'] > 85]
        ltxt = ' '.join(w['text'] for w in left[:8])
        print(f'  p{i+1} | left: {ltxt[:60]} | right: {txt[:80]}')
