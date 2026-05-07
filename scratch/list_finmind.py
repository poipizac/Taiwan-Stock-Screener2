from FinMind.data import DataLoader

def list_dl_methods():
    dl = DataLoader()
    methods = [m for m in dir(dl) if not m.startswith('_')]
    print("\n".join(methods))

if __name__ == "__main__":
    list_dl_methods()
