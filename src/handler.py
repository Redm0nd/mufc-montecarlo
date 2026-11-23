"""
Monte Carlo season simulator for the Premier League.

Triggered by SQS. Each message asks for a simulation of the rest of the
season for one team, e.g.

    {"request_id": "mufc-2026-11-23", "season": "2627", "team": "Man United",
     "bootstrap_fits": 2000, "sims_per_fit": 10000}

Results are written to S3 as JSON. Data comes from football-data.co.uk.
"""
import csv
import io
import json
import logging
import os
import time
import urllib.request
from collections import Counter

import boto3
import numpy as np
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3")
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
DATA_URL = "https://www.football-data.co.uk/mmz4281/{season}/E0.csv"

# Stop starting new bootstrap fits when we're this close to the timeout,
# so we always have time to write partial results instead of being killed.
SAFETY_MARGIN_MS = int(os.environ.get("SAFETY_MARGIN_MS", "60000"))


# --------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------
def load_results(season):
    """Return list of (home, away, home_goals, away_goals) for played matches."""
    url = DATA_URL.format(season=season)
    with urllib.request.urlopen(url, timeout=30) as resp:
        text = resp.read().decode("utf-8-sig", errors="replace")
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        if r.get("HomeTeam") and r.get("FTHG") not in (None, ""):
            rows.append((r["HomeTeam"], r["AwayTeam"], int(r["FTHG"]), int(r["FTAG"])))
    return rows


def remaining_fixtures(teams, played):
    """Every team plays every other team home and away. Anything not yet
    played is still to come - no fixtures feed needed."""
    done = {(h, a) for h, a, _, _ in played}
    return [(h, a) for h in teams for a in teams if h != a and (h, a) not in done]


# --------------------------------------------------------------------------
# Model: independent Poisson goals with team attack/defence + home advantage
# (Maher 1982), fitted by iterative proportional fitting.
# --------------------------------------------------------------------------
def fit(hi, ai, hg, ag, n_teams, iters=50, prior=2.0):
    """`prior` adds a couple of pseudo-matches against an average side, so a
    bootstrap sample where a team happens to score 0 doesn't give it zero
    attack (and early-season data doesn't get over-trusted)."""
    base = (hg.sum() + ag.sum()) / (2 * len(hg))      # league goals per team per match
    attack = np.ones(n_teams)
    defence = np.ones(n_teams)
    home = 1.3
    for _ in range(iters):
        scored = np.bincount(hi, hg, n_teams) + np.bincount(ai, ag, n_teams)
        exposure = (np.bincount(hi, home * defence[ai], n_teams)
                    + np.bincount(ai, defence[hi], n_teams))
        attack = (scored + prior * base) / (exposure + prior)
        attack /= attack.mean()

        conceded = np.bincount(ai, hg, n_teams) + np.bincount(hi, ag, n_teams)
        exposure = (np.bincount(ai, home * attack[hi], n_teams)
                    + np.bincount(hi, attack[ai], n_teams))
        defence = (conceded + prior * base) / (exposure + prior)

        home = hg.sum() / (attack[hi] * defence[ai]).sum()
    return attack, defence, home


def simulate(attack, defence, home, base_pts, base_gd, fx_h, fx_a, n_sims, rng, team):
    """Simulate the remaining fixtures n_sims times. Returns finishing
    position (1-based) of every team in every simulation: shape (n_sims, n_teams),
    plus the team's result (+1 win, 0 draw, -1 loss) in each of its own
    remaining fixtures: shape (n_sims, n_team_fixtures)."""
    n_teams = len(attack)
    lam_h = home * attack[fx_h] * defence[fx_a]
    lam_a = attack[fx_a] * defence[fx_h]
    gh = rng.poisson(lam_h, size=(n_sims, len(fx_h)))
    ga = rng.poisson(lam_a, size=(n_sims, len(fx_h)))

    hw, dr, aw = gh > ga, gh == ga, gh < ga
    pts = np.tile(base_pts, (n_sims, 1)).astype(float)
    gd = np.tile(base_gd, (n_sims, 1)).astype(float)
    for j in range(len(fx_h)):
        h, a = fx_h[j], fx_a[j]
        pts[:, h] += 3 * hw[:, j] + dr[:, j]
        pts[:, a] += 3 * aw[:, j] + dr[:, j]
        gd[:, h] += gh[:, j] - ga[:, j]
        gd[:, a] += ga[:, j] - gh[:, j]

    # rank on points, then goal difference, then a random tiebreak
    key = pts * 1e6 + gd * 1e2 + rng.random(pts.shape)
    order = np.argsort(-key, axis=1)
    pos = np.empty_like(order)
    np.put_along_axis(pos, order, np.arange(1, n_teams + 1)[None, :].repeat(n_sims, 0), axis=1)

    mine = np.where((fx_h == team) | (fx_a == team))[0]
    sign = np.where(fx_h[mine] == team, 1, -1)
    results = np.sign(gh[:, mine] - ga[:, mine]) * sign
    return pos, results


# --------------------------------------------------------------------------
# One request
# --------------------------------------------------------------------------
def run(job, context):
    season, team = job["season"], job["team"]
    bootstrap_fits = int(job.get("bootstrap_fits", 2000))
    sims_per_fit = int(job.get("sims_per_fit", 10000))

    played = load_results(season)
    teams = sorted({t for m in played for t in m[:2]})
    if team not in teams:
        raise ValueError(f"{team!r} not found. Teams: {teams}")
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)

    hi = np.array([idx[m[0]] for m in played])
    ai = np.array([idx[m[1]] for m in played])
    hg = np.array([m[2] for m in played], dtype=float)
    ag = np.array([m[3] for m in played], dtype=float)

    base_pts = (np.bincount(hi, 3 * (hg > ag) + (hg == ag), n)
                + np.bincount(ai, 3 * (ag > hg) + (hg == ag), n))
    base_gd = np.bincount(hi, hg - ag, n) + np.bincount(ai, ag - hg, n)

    fx = remaining_fixtures(teams, played)
    fx_h = np.array([idx[h] for h, _ in fx])
    fx_a = np.array([idx[a] for _, a in fx])
    log.info("season=%s played=%d remaining=%d teams=%d", season, len(played), len(fx), n)

    t = idx[team]
    mine = np.where((fx_h == t) | (fx_a == t))[0]
    my_fixtures = [fx[j] for j in mine]
    # top-4 / relegated counts and totals for each of my fixtures x outcome (L, D, W)
    top4_given = np.zeros((len(mine), 3))
    rel_given = np.zeros((len(mine), 3))
    seen_given = np.zeros((len(mine), 3))

    rng = np.random.default_rng(job.get("seed"))
    finishes = Counter()
    fits_done = 0
    started = time.time()

    # Bootstrap: resample the played matches, refit, simulate. This is what
    # captures "we're not sure how good anyone actually is" and it's also
    # what makes the job long-running.
    for b in range(bootstrap_fits):
        if context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS:
            log.warning("Stopping early at fit %d/%d to avoid timeout", b, bootstrap_fits)
            break
        sample = rng.integers(0, len(played), len(played))
        att, dfn, home = fit(hi[sample], ai[sample], hg[sample], ag[sample], n)
        pos, res = simulate(att, dfn, home, base_pts, base_gd, fx_h, fx_a, sims_per_fit, rng, t)
        my_pos = pos[:, t]
        finishes.update({int(p): int(c) for p, c in zip(*np.unique(my_pos, return_counts=True))})
        top4 = my_pos <= 4
        relegated = my_pos >= n - 2
        for k, outcome in enumerate((-1, 0, 1)):
            hit = res == outcome                      # (sims, my fixtures)
            seen_given[:, k] += hit.sum(axis=0)
            top4_given[:, k] += (hit & top4[:, None]).sum(axis=0)
            rel_given[:, k] += (hit & relegated[:, None]).sum(axis=0)
        fits_done += 1
        if fits_done % 25 == 0:
            log.info("fit %d/%d done, %.0fs elapsed, %ds left",
                     fits_done, bootstrap_fits, time.time() - started,
                     context.get_remaining_time_in_millis() // 1000)

    if fits_done == 0:
        raise TimeoutError("No time left to run a single fit - check the function timeout")
    total = sum(finishes.values())
    dist = {p: finishes[p] / total for p in range(1, n + 1)}

    # "Which games matter most": how much each result moves P(top 4) and
    # P(relegated). swing = P(x | win) - P(x | loss) for top 4 and
    # P(x | loss) - P(x | win) for relegation, so bigger always = matters more.
    with np.errstate(invalid="ignore", divide="ignore"):
        p_top4 = np.nan_to_num(top4_given / seen_given)
        p_rel = np.nan_to_num(rel_given / seen_given)
    games = []
    for j, (h, a) in enumerate(my_fixtures):
        games.append({"fixture": f"{h} v {a}",
                      "p_top_4_if_win": float(p_top4[j, 2]), "p_top_4_if_loss": float(p_top4[j, 0]),
                      "top_4_swing": float(p_top4[j, 2] - p_top4[j, 0]),
                      "p_relegated_if_win": float(p_rel[j, 2]),
                      "p_relegated_if_draw": float(p_rel[j, 1]),
                      "p_relegated_if_loss": float(p_rel[j, 0]),
                      "relegation_swing": float(p_rel[j, 0] - p_rel[j, 2])})
    return {
        "request_id": job["request_id"],
        "season": season,
        "team": team,
        "matches_played": len(played),
        "fixtures_remaining": len(fx),
        "bootstrap_fits_completed": fits_done,
        "bootstrap_fits_requested": bootstrap_fits,
        "simulations": total,
        "elapsed_seconds": round(time.time() - started, 1),
        "current_points": int(base_pts[idx[team]]),
        "p_title": dist[1],
        "p_top_4": sum(dist[p] for p in range(1, 5)),
        "p_top_6": sum(dist[p] for p in range(1, 7)),
        "p_relegation": sum(dist[p] for p in range(n - 2, n + 1)),
        "expected_position": sum(p * v for p, v in dist.items()),
        "position_distribution": dist,
        "biggest_games_top_4": sorted(games, key=lambda g: -g["top_4_swing"])[:10],
        "biggest_games_relegation": sorted(games, key=lambda g: -g["relegation_swing"])[:10],
    }


# --------------------------------------------------------------------------
# Lambda entrypoint (SQS, with partial batch failures)
# --------------------------------------------------------------------------
def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            job = json.loads(record["body"])
            key = f"results/{job['request_id']}.json"

            # Idempotency: SQS is at-least-once and a 30 minute run is a big
            # window for a duplicate. If the result already exists, skip.
            try:
                s3.head_object(Bucket=RESULTS_BUCKET, Key=key)
                log.info("Result %s already exists, skipping duplicate", key)
                continue
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                    raise

            result = run(job, context)
            try:
                # If-None-Match: * means a racing duplicate can't overwrite us
                s3.put_object(Bucket=RESULTS_BUCKET, Key=key, IfNoneMatch="*",
                              Body=json.dumps(result, indent=2),
                              ContentType="application/json")
            except ClientError as e:
                if e.response["Error"]["Code"] != "PreconditionFailed":
                    raise
            log.info("Wrote s3://%s/%s p_top_4=%.3f", RESULTS_BUCKET, key, result["p_top_4"])
        except Exception:
            log.exception("Failed processing message %s", record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
