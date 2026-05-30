from datasets import load_dataset


def ingest(limit:int , run_id: str):
    ds = load_dataset("rootsautomation/RICO-Screen2Words", split="train", streaming=True, trust_remote_code=True)
    peek = next(iter(ds))

    print("row keys:", list(peek.keys()))
    print("first row:", peek["screenId"], "|", peek["app_package_name"], "|", peek["category"])
    print("image type:", type(peek["image"]).__name__, "size:", peek["image"].size)

    

