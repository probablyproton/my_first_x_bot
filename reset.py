import os
path = "state.json"
if os.path.exists(path):
    os.remove(path)
    print("state.json deleted")
else:
    print("state.json not found, nothing to delete")
