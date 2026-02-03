import yaml
from pathlib import Path
from app.services.policy.schema import PolicyBundle


def load_policy_from_file(path: str) -> PolicyBundle:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    policy = PolicyBundle(**raw)
    return policy
