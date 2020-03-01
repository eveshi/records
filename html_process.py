import re

a_tag_pattern = r'<a[^>]*>[^<]*</a>'


def extract_tag(match):
    ori = match.group(0)
    seq = re.search(r'>[^<]*<', ori)[0]

    return re.sub(r'[<>]', '', seq)


def extract_a_tag(s):
    return re.sub(a_tag_pattern, extract_tag, s)


def replace_br_tag(s):
    return re.sub('<br />', '\n', s)


def process_html(s):
    steps = [
        replace_br_tag,
        extract_a_tag,
    ]

    for step in steps:
        s = step(s)

    return s

