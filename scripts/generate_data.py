import csv, random, datetime

random.seed(42)

VILLES = ["Dakar","Thiès","Saint-Louis","Ziguinchor","Kaolack",
          "Abidjan","Bamako","Lomé","Cotonou","Ouagadougou"]
PRENOMS_H = ["Ibrahima","Mamadou","Ousmane","Abdou","Aliou",
             "Moussa","Cheikh","Seydou","Modou","Lamine"]
PRENOMS_F = ["Fatou","Aminata","Mariama","Aïssatou","Rokhaya",
             "Khady","Ndèye","Adja","Coumba","Mame"]
NOMS = ["Diop","Fall","Ndiaye","Sow","Diallo","Sy","Traoré",
        "Koné","Ba","Sarr","Gueye","Diouf"]
STATUTS = ["ACTIVE","CLOSED","DEFAULTED","OVERPAID"]
WEIGHTS = [0.55, 0.30, 0.10, 0.05]
DEVISES  = ["XOF","XOF","XOF","EUR"]

def rand_date(s, e):
    return s + datetime.timedelta(days=random.randint(0, (e-s).days))
fmt = lambda d: d.strftime("%Y-%m-%d")

clients = []
for i in range(1, 301):
    g = random.choice(["M","F"])
    clients.append({
        "client_id": f"CLI{i:04d}",
        "prenom": random.choice(PRENOMS_H if g=="M" else PRENOMS_F),
        "nom": random.choice(NOMS),
        "date_naissance": fmt(rand_date(datetime.date(1965,1,1), datetime.date(2000,12,31))),
        "genre": g,
        "telephone": f"+221 7{random.randint(0,9)} {random.randint(100,999)} {random.randint(10,99)} {random.randint(10,99)}",
        "ville": random.choice(VILLES),
        "date_inscription": fmt(rand_date(datetime.date(2019,1,1), datetime.date(2024,1,1))),
    })

loans = []
for i in range(1, 601):
    c = random.choice(clients)
    d_dec = rand_date(datetime.date(2020,1,1), datetime.date(2024,6,1))
    duree = random.choice([6,12,18,24,36])
    loans.append({
        "loan_id": f"LN{i:05d}",
        "client_id": c["client_id"],
        "agent_id": f"AGT{random.randint(1,20):03d}",
        "montant_principal": round(random.uniform(50000,2500000)/5000)*5000,
        "taux_interet": round(random.uniform(8,24),1),
        "date_decaissement": fmt(d_dec),
        "date_echeance": fmt(d_dec + datetime.timedelta(days=duree*30)),
        "duree_mois": duree,
        "statut": random.choices(STATUTS, WEIGHTS)[0],
        "devise": random.choice(DEVISES),
    })

repayments = []
r_id = 1
for loan in loans:
    s = loan["statut"]
    if s == "ACTIVE":    nb = random.randint(1, loan["duree_mois"]//2)
    elif s == "CLOSED":  nb = loan["duree_mois"]
    elif s == "DEFAULTED": nb = random.randint(0,3)
    else:                nb = loan["duree_mois"] + random.randint(1,2)
    d0 = datetime.date.fromisoformat(loan["date_decaissement"])
    m0 = round(loan["montant_principal"]/loan["duree_mois"]/500)*500
    for m in range(nb):
        d_pay = d0 + datetime.timedelta(days=(m+1)*30 + random.randint(-3,5))
        repayments.append({
            "repayment_id": f"REP{r_id:06d}",
            "loan_id": loan["loan_id"],
            "montant": m0,
            "date_paiement": fmt(d_pay),
            "statut_paiement": "PAID" if random.random()>0.05 else "LATE",
        })
        r_id += 1

OUT = "/home/claude/elt_demo/data/raw"

def write_csv(name, rows, fields):
    with open(f"{OUT}/{name}","w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

write_csv("clients.csv", clients,
    ["client_id","prenom","nom","date_naissance","genre","telephone","ville","date_inscription"])
write_csv("loans.csv", loans,
    ["loan_id","client_id","agent_id","montant_principal","taux_interet",
     "date_decaissement","date_echeance","duree_mois","statut","devise"])
write_csv("repayments.csv", repayments,
    ["repayment_id","loan_id","montant","date_paiement","statut_paiement"])

print(f"clients   : {len(clients)}")
print(f"loans     : {len(loans)}")
print(f"repayments: {len(repayments)}")
