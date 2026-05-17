import argparse
import pandas as pd
import time

FILEPATH = './doc.csv'

def mode_1(args):
    df = pd.read_csv(FILEPATH)

    if args.filter is not None:
        for filter in args.filter.split(","):
            if filter.find("=") != -1:
                col, val = filter.split("=")
                print(col, val)
                df = df[df[col] == val]
            elif filter.find(">") != -1:
                col, val = filter.split(">")
                df = df[df[col] > int(val)]   
            elif filter.find("<") != -1:
                col, val = filter.split("<")
                df = df[df[col] < int(val)]
            else:
                print(f"WARN: Unrecognized filter {filter}" )

    if args.sort is not None:
        sorts = []
        ascs = []
        for sort in args.sort.split(","):
            sort, asc = sort.split(".")
            sorts.append(sort)
            ascs.append(asc == '1')

        df = df.sort_values(by=sorts, ascending=ascs)

    df.to_csv(args.output)


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