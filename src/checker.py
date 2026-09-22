from utils import check_data_completeness

if __name__ == "__main__":
    missing = check_data_completeness()
    if not missing:
        # Exit code 0: Data is complete
        exit(0)
    else:
        # Print only the comma-separated list for the .bat file to capture
        print("\n" + ",".join(missing))
        # Exit code 1: Some data is missing
        exit(1)
