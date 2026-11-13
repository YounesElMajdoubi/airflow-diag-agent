"""Notation humaine à l'aveugle des diagnostics tirés par `make verif-juge`."""
import csv
p = "eval/verification/feuille.csv"
rows = list(csv.DictReader(open(p, encoding="utf-8")))
def ask(q):
    while (v := input(q).strip()) not in ("0", "1", "2"):
        print("  -> 0, 1 ou 2")
    return v
for r in rows:
    if r["note_humaine_diagnostic"]:
        continue
    print("\n" + "=" * 90)
    print(f"#{r['id']}  ({r['scenario']})")
    print(f"CAUSE RÉELLE : {r['cause_reelle']}\n")
    print(f"DIAGNOSTIC   : {r['diagnostic_agent']}\n")
    print(f"CORRECTION   : {r['correction_agent']}\n")
    r["note_humaine_diagnostic"] = ask("Note diagnostic (0/1/2) : ")
    r["note_humaine_correction"] = ask("Note correction (0/1/2) : ")
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, rows[0].keys()); w.writeheader(); w.writerows(rows)
print("\nTerminé.")
