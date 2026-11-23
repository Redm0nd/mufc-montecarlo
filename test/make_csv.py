# Convert openfootball JSON into football-data.co.uk E0.csv columns for local testing
import json, csv, sys
names = {"Manchester United FC":"Man United","Manchester City FC":"Man City","Nottingham Forest FC":"Nott'm Forest",
         "Tottenham Hotspur FC":"Tottenham","Wolverhampton Wanderers FC":"Wolves","Newcastle United FC":"Newcastle",
         "Brighton & Hove Albion FC":"Brighton","West Ham United FC":"West Ham","AFC Bournemouth":"Bournemouth",
         "Leeds United FC":"Leeds","Hull City AFC":"Hull","Ipswich Town FC":"Ipswich","Coventry City FC":"Coventry"}
n = lambda t: names.get(t, t.replace(" FC","").replace(" AFC",""))
src, out = sys.argv[1], sys.argv[2]
m = json.load(open(src))["matches"]
with open(out, "w", newline="") as f:
    w = csv.writer(f); w.writerow(["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG"])
    for x in m:
        s = x.get("score")
        if isinstance(s, dict) and s.get("ft"):
            w.writerow(["E0", x["date"], n(x["team1"]), n(x["team2"]), *s["ft"]])
