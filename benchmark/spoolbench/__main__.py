"""
Command line:

  python -m spoolbench run --profile scenarios/quick.yaml --jar main=../target/aws.greengrass.DiskSpooler.jar \
                           [--jar pr=other.jar] [--only REGEX] [--repeat N] --out results/<name>
  python -m spoolbench report results/<name>            # (re)build report.json / report.md / charts
  python -m spoolbench compare BASELINE.json CANDIDATE.json [--baseline-label L] [--candidate-label L] [--out F]
  python -m spoolbench list --profile scenarios/quick.yaml [--only REGEX]
  python -m spoolbench gen-proto
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path


def _labelled(values, what):
    result = {}
    for value in values or []:
        if "=" not in value:
            sys.exit(f"--{what} expects LABEL=VALUE, got {value!r}")
        label, path = value.split("=", 1)
        result[label] = path
    return result


def cmd_run(args) -> int:
    from . import config, environment, nucleus, payloads, protos, report
    from .runner import ScenarioRun

    protos.generate()
    profile, settings, scenarios = config.load(args.profile)
    if args.java_home:
        settings.java_home = args.java_home
    settings.java_home = settings.java_home or os.environ.get("JAVA_HOME")
    if args.only:
        scenarios = [s for s in scenarios if re.search(args.only, s.name)]
    if not scenarios:
        sys.exit("no scenario selected")
    jars = {label: Path(path).resolve() for label, path in _labelled(args.jar, "jar").items()}
    if not jars:
        sys.exit("give at least one --jar LABEL=PATH")
    for label, jar in jars.items():
        if not jar.exists():
            sys.exit(f"{label}: {jar} does not exist")
    refs = _labelled(args.ref, "ref")

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    distribution = nucleus.install_distribution(settings.nucleus_version, args.cache_dir)
    pool = payloads.PayloadPool(seed=settings.seed, profile=payloads.PayloadProfile.from_dict(settings.payload_profile))
    meta = {
        "profile_file": str(args.profile),
        "profile": profile,
        "environment": environment.collect(settings.java_home, settings.nucleus_version),
        "payloads": payloads.size_stats(pool.sizes),
        "labels": {label: {"jar": str(jar), "ref": refs.get(label, "")} for label, jar in jars.items()},
    }
    meta_path = out / "meta.json"
    if meta_path.exists():  # several invocations (e.g. one per CI job) may share an output directory
        previous = json.loads(meta_path.read_text())
        meta["labels"] = {**previous.get("labels", {}), **meta["labels"]}
    meta_path.write_text(json.dumps(meta, indent=2))

    labels = list(jars)
    failures = 0
    for repeat in range(args.repeat):
        for index, scenario in enumerate(scenarios):
            # alternate the order of the builds, so that drift of the machine does not favour one of them
            order = labels if (repeat + index) % 2 == 0 else list(reversed(labels))
            for label in order:
                directory = out / label / (scenario.name if args.repeat == 1 else f"{scenario.name}-rep{repeat + 1}")
                print(f"=== {label}: {scenario.name} (repeat {repeat + 1}/{args.repeat})", flush=True)
                result = ScenarioRun(scenario, settings, jars[label], label, directory, distribution,
                                     keep_workdir=args.keep_workdir).run()
                result["repeat"] = repeat + 1
                (directory / "result.json").write_text(json.dumps(result, indent=2, default=str))
                failures += result["status"] != "ok"
    report.build(out)
    return 1 if failures and args.fail_on_error else 0


def cmd_report(args) -> int:
    from . import report

    report.build(args.results)
    return 0


def cmd_compare(args) -> int:
    from . import compare

    text, worse = compare.compare_files(args.baseline, args.candidate, args.baseline_label, args.candidate_label)
    if args.out:
        args.out.write_text(text)
    print(text)
    return 1 if worse and args.fail_on_regression else 0


def cmd_list(args) -> int:
    from . import config

    _, _, scenarios = config.load(args.profile)
    names = [s.name for s in scenarios if not args.only or re.search(args.only, s.name)]
    print(json.dumps(names))
    return 0


def cmd_gen_proto(_args) -> int:
    from . import protos

    print(protos.generate(force=True))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="spoolbench")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run scenarios")
    run.add_argument("--profile", type=Path, required=True)
    run.add_argument("--jar", action="append", help="LABEL=path to aws.greengrass.DiskSpooler.jar (repeatable)")
    run.add_argument("--ref", action="append", help="LABEL=git ref / commit description shown in the report")
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--only", help="regular expression on scenario names")
    run.add_argument("--repeat", type=int, default=1)
    run.add_argument("--java-home", help="JDK used to run the Nucleus (default: $JAVA_HOME)")
    run.add_argument("--cache-dir", type=Path, default=Path.home() / ".cache" / "spoolbench")
    run.add_argument("--keep-workdir", action="store_true")
    run.add_argument("--fail-on-error", action="store_true", help="exit 1 if a scenario failed to run")
    run.set_defaults(func=cmd_run)

    rep = sub.add_parser("report", help="build report.json, report.md and charts from a results directory")
    rep.add_argument("results", type=Path)
    rep.set_defaults(func=cmd_report)

    cmp = sub.add_parser("compare", help="compare two report.json files (or two labels of one)")
    cmp.add_argument("baseline", type=Path)
    cmp.add_argument("candidate", type=Path, nargs="?")
    cmp.add_argument("--baseline-label")
    cmp.add_argument("--candidate-label")
    cmp.add_argument("--out", type=Path)
    cmp.add_argument("--fail-on-regression", action="store_true")
    cmp.set_defaults(func=cmd_compare)

    lst = sub.add_parser("list", help="print the scenario names of a profile as a JSON list")
    lst.add_argument("--profile", type=Path, required=True)
    lst.add_argument("--only", help="regular expression on scenario names")
    lst.set_defaults(func=cmd_list)

    gen = sub.add_parser("gen-proto", help="compile the IBEMS protobuf schema")
    gen.set_defaults(func=cmd_gen_proto)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
