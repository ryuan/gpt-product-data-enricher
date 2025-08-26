import re
from dataclasses import dataclass, field
from typing import List, Tuple, Optional


@dataclass
class TagBlock:
    """One <...> ... </> block."""
    tag_items: List[str]
    brace_groups: List[List[str]] = field(default_factory=list)  # each {...} list found inside the block
    start_index: int = -1
    end_index: int = -1

class TagParseError(Exception):
    pass


def parse_custom_tags(notes: str) -> Tuple[bool, Optional[List[TagBlock]], List[str]]:
    """
    Parse and validate a string containing zero or more blocks like:
        <Finish, Fabric, Wood>  ... {Finish, Fabric} ... </>
    - Opening tags are <...> (list of strings, each string being a possible word in field names).
    - Closing tag is exactly '</>'.
    - Inside a tag block, there may be zero or more {...} lists of strings, each string being a possible word in field names.
    - Tags must not nest (since the closing tag is generic).

    Returns: (is_valid, blocks_or_None, errors)
    """

    i, n = 0, len(notes)
    blocks: List[TagBlock] = []
    errors: List[str] = []
    stack: List[TagBlock] = []

    def fail(msg: str, at: int):
        errors.append(f"{msg} (at index {at})")

    while i < n:
        ch = notes[i]

        # Detect closing tag </>
        if ch == "<" and notes.startswith("</>", i):
            if not stack:
                fail("Unexpected closing tag </> with no open tag", i)
                i += 3
                continue
            blk = stack.pop()
            blk.end_index = i + 3
            blocks.append(blk)
            i += 3
            continue

        # Detect opening tag < ... >
        if ch == "<":
            # Find matching '>' (not including a closing tag literal which is handled above)
            j = notes.find(">", i + 1)

            if j == -1:
                fail("Unclosed opening tag '<...>'", i)
                break

            raw = notes[i:j+1]

            try:
                items = __split_list(raw, "<", ">")
            except TagParseError as e:
                fail(str(e), i)
                # Still advance to avoid infinite loop
                i = j + 1
                continue

            # Forbid nesting (since </> is generic, nesting is ambiguous)
            if stack:
                fail("Nested opening tags are not allowed", i)

            blk = TagBlock(tag_items=items, start_index=i)
            stack.append(blk)
            i = j + 1
            continue

        # Detect a {...} list only inside an open tag block
        if ch == "{":
            k = notes.find("}", i + 1)

            if k == -1:
                fail("Unclosed curly list '{...}'", i)
                break

            if not stack:
                fail("Curly list '{...}' must appear inside an open tag block", i)
                # still advance
                i = k + 1
                continue

            raw = notes[i:k+1]

            try:
                items = __split_list(raw, "{", "}")
            except TagParseError as e:
                fail(str(e), i)
                i = k + 1
                continue

            stack[-1].brace_groups.append(items)
            i = k + 1
            continue

        # Lone '}' without opening '{'
        if ch == "}":
            fail("Stray '}' without matching '{'", i)
            i += 1
            continue

        # Otherwise, just move on
        i += 1

    # End-of-string checks
    if stack:
        # Any still-open blocks are errors
        for blk in stack:
            fail("Missing closing tag '</>' for opening tag", blk.start_index)

    is_valid = len(errors) == 0

    return (is_valid, blocks if is_valid else None, errors)

def optimize_notes(notes: str, fields: List[str]) -> str:
    """
    Process custom-tagged notes according to rules:
    - If any opening tag item matches a provided field (substring, case-insensitive), keep the block:
        - Replace each {...} inside using matching field names, joined with 'or' before the last.
        - Remove the <...> and </> markers.
      Else, delete the entire tagged block.
    - Text outside tags is left unchanged.
    """

    tag_pat = re.compile(r"<([^<>]+)>(.*?)</>", re.DOTALL)

    result_parts: List[str] = []
    last = 0

    for m in tag_pat.finditer(notes):
        # Append text before the block unchanged
        result_parts.append(notes[last:m.start()])

        raw_items = m.group(1)
        content = m.group(2)

        tag_items = [x.strip() for x in raw_items.split(",") if x.strip()]
        # Determine if this block is relevant (any tag item is a substring of any field)
        block_fields = __fields_matching_any_keywords(tag_items, fields)

        if block_fields:
            # Replace curly groups; remove the tag wrappers
            replaced = __replace_curly_groups(content, fields)
            result_parts.append(replaced)
        else:
            # Drop whole block
            pass

        last = m.end()

    # Append remaining tail
    result_parts.append(notes[last:])

    # Light cleanup: collapse spaces created by removals around braces/tags
    optimized_notes = "".join(result_parts)

    # Remove extra spaces before punctuation and tidy double spaces that can arise
    optimized_notes = re.sub(r"[ \t]+([,.!?;:])", r"\1", optimized_notes)
    optimized_notes = re.sub(r"[ \t]{2,}", " ", optimized_notes)
    # Trim trailing spaces on each line
    optimized_notes = "\n".join(line.rstrip() for line in optimized_notes.splitlines())

    return optimized_notes

def __split_list(raw: str, opener: str, closer: str) -> List[str]:
    """
    Split a comma-separated list inside delimiters, trimming whitespace.
    Allows empty whitespace around commas; forbids empty items.
    """

    inner = raw.strip()

    if not (inner.startswith(opener) and inner.endswith(closer)):
        raise TagParseError(f"Expected {opener}...{closer}, got: {raw!r}")
    
    body = inner[len(opener): -len(closer)].strip()

    if body == "":
        # Empty list is allowed? Adjust as needed. Here we allow empty -> []
        return []
    
    items = [item.strip() for item in body.split(",")]

    if any(item == "" for item in items):
        raise TagParseError(f"Empty item in list: {raw!r}")
    
    return items

def __english_join(items: List[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} or {items[1]}"
    return f"{', '.join(items[:-1])}, or {items[-1]}"

def __fields_matching_any_keywords(keywords: List[str], fields: List[str]) -> List[str]:
    """
    Return fields that contain ANY of the keywords as substrings (case-insensitive), preserving fields order and deduping.
    """

    seen = set()
    out: List[str] = []
    lower_fields = [(f, f.casefold()) for f in fields]
    lowers = [k.casefold() for k in keywords]

    for f_orig, f_low in lower_fields:
        if any(k in f_low for k in lowers):
            if f_orig not in seen:
                seen.add(f_orig)
                out.append(f_orig)

    return out

def __replace_curly_groups(text: str, fields: List[str]) -> str:
    """
    Replace each {a, b, c} with matching field names. If none match, remove the braces entirely.
    """

    curly_pat = re.compile(r"\{([^{}]*)\}")

    def repl(m: re.Match) -> str:
        inner = m.group(1)
        items = [x.strip() for x in inner.split(",") if x.strip()]
        matches = __fields_matching_any_keywords(items, fields)

        return __english_join(matches)

    return curly_pat.sub(repl, text)