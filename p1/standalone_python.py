import argparse
import pandas as pd
import time

FILEPATH = './doc.csv'

def mode_1(args):
    df = pd.read_csv(FILEPATH)

    if args.filter is not None:
        for filter_expr in args.filter.split(","):
            if "=" in filter_expr:
                col, val = filter_expr.split("=", 1)
                col = col.strip()
                val = val.strip()

                df = df[
                    df[col].astype(str).str.lower() == val.lower()
                ]

            elif ">" in filter_expr:
                col, val = filter_expr.split(">", 1)
                col = col.strip()
                val = float(val.strip())

                df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df[df[col] > val]

            elif "<" in filter_expr:
                col, val = filter_expr.split("<", 1)
                col = col.strip()
                val = float(val.strip())

                df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df[df[col] < val]

            else:
                print(f"WARN: Unrecognized filter {filter_expr}")

    if args.sort is not None:
        sorts = []
        ascs = []
        for sort in args.sort.split(","):
            sort, asc = sort.split(".")
            sorts.append(sort)
            ascs.append(asc == '1')

        df = df.sort_values(by=sorts, ascending=ascs)

    if args.count is not None:
        count_df = pd.DataFrame({
            "count": [len(df)]
        })
        count_df.to_csv(args.output, index=False)
    else:
        df.to_csv(args.output, index=False)


def mode_2(args):
    print(args)


def main(args):
    start_time = time.perf_counter()
    if args.mode == '1':
        mode_1(args)
    elif args.mode == '2':
        mode_2(args)
    else:
        print("ERROR: Unsupported mode " + args.mode)

    end_time = time.perf_counter()
    print(f"Elapsed time for mode {args.mode}: {end_time - start_time}s")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode')
    parser.add_argument('--filter')
    parser.add_argument('--sort')
    parser.add_argument('--count')
    parser.add_argument('--output')

    args = parser.parse_args()
    main(args)