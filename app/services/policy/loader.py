import yaml
from pathlib import Path
from app.services.policy.schema import PolicyBundle


def load_policy_from_file(path: str) -> PolicyBundle:

    p = Path(path)

    if not p.exists():
        raise RuntimeError(f"Policy file not found: {path}")

    with open(p, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    policy = PolicyBundle(**raw)
    return policy
