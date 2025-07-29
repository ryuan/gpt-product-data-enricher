# Prints a list of string options with index numbers for user prompt input
def print_options(options: list[str]) -> None:
    print("\n")
    for i, option in enumerate(options):
        print(f"[{i}] {option}")