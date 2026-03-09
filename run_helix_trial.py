import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from epl.traces.public_trial import main as helix_trial_main


if __name__ == "__main__":
    raise SystemExit(helix_trial_main())
