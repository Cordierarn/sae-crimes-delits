"""
Phase 3 — Migration PostgreSQL → Neo4j
Crimes et Délits 2012-2021

Prérequis :
  - Neo4j Desktop démarré (bolt://localhost:7687)
  - pip install neo4j psycopg2-binary

Structure :
  Phase 0 — Contraintes & index
  Phase 1 — Noeuds (Force, Direction, Departement, Service, TypeFait)
  Phase 2 — Relations (APPARTIENT_A, DEPEND_DE, SITUE_DANS, ENREGISTRE)
  Phase 3 — Enrichissement (Region, Categorie + leurs relations)
"""

import time
import psycopg2
from neo4j import GraphDatabase

# ── Configuration ────────────────────────────────────────
PG = dict(host="localhost", port=5432, dbname="crimes_delits",
          user="postgres", password="user")

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "crimes_delits"

BATCH_SIZE = 500   # Noeuds/relations insérés par transaction

# ─────────────────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────────────────

def chrono(label, fn, *args, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    dt = time.perf_counter() - t0
    print(f"  [OK] {label:<50s} {dt:6.1f}s")
    return result

def run_batch(session, cypher, rows, batch_size=BATCH_SIZE):
    """Exécute une requête Cypher par lots."""
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        session.run(cypher, rows=batch)
        total += len(batch)
    return total

# ─────────────────────────────────────────────────────────
# PHASE 0 — Contraintes et index Neo4j
# Justification : garantir l'unicité des noeuds et
# accélérer les MERGE lors du chargement.
# ─────────────────────────────────────────────────────────

CONTRAINTES = [
    # Unicité — évite les doublons lors des MERGE
    "CREATE CONSTRAINT force_code        IF NOT EXISTS FOR (n:Force)       REQUIRE n.code         IS UNIQUE",
    "CREATE CONSTRAINT direction_code    IF NOT EXISTS FOR (n:Direction)    REQUIRE n.code         IS UNIQUE",
    "CREATE CONSTRAINT departement_code  IF NOT EXISTS FOR (n:Departement)  REQUIRE n.code_dept    IS UNIQUE",
    "CREATE CONSTRAINT service_id        IF NOT EXISTS FOR (n:Service)      REQUIRE n.id_service   IS UNIQUE",
    "CREATE CONSTRAINT typefait_code     IF NOT EXISTS FOR (n:TypeFait)     REQUIRE n.code_index   IS UNIQUE",
    "CREATE CONSTRAINT region_code       IF NOT EXISTS FOR (n:Region)       REQUIRE n.code_region  IS UNIQUE",
    "CREATE CONSTRAINT categorie_code    IF NOT EXISTS FOR (n:Categorie)    REQUIRE n.code         IS UNIQUE",
]

def phase0_contraintes(driver):
    print("\n[PHASE 0] Contraintes et index")
    with driver.session() as s:
        for cql in CONTRAINTES:
            s.run(cql)
            label = cql.split("FOR")[1].split("REQUIRE")[0].strip()
            print(f"  [OK] Contrainte : {label}")

# ─────────────────────────────────────────────────────────
# PHASE 1 — Chargement des noeuds
# Justification : on charge d'abord toutes les entités
# indépendantes (sans FK) puis celles qui en dépendent.
# ─────────────────────────────────────────────────────────

def phase1_noeuds(driver, pg):
    print("\n[PHASE 1] Chargement des noeuds")
    cur = pg.cursor()

    # ── Force ──────────────────────────────────────────
    # Justification : noeud racine, aucune dépendance
    cur.execute("SELECT id_force, code, libelle FROM force")
    rows = [{"id": r[0], "code": r[1], "libelle": r[2]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("Force (2 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (n:Force {code: r.code}) "
            "SET n.id_force = r.id, n.libelle = r.libelle",
            rows)

    # ── Direction ──────────────────────────────────────
    # Justification : liée à PN uniquement, pas de dépendance de noeud
    cur.execute("SELECT id_direction, code, libelle FROM direction")
    rows = [{"id": r[0], "code": r[1], "libelle": r[2] or r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("Direction (11 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (n:Direction {code: r.code}) "
            "SET n.id_direction = r.id, n.libelle = r.libelle",
            rows)

    # ── Departement ────────────────────────────────────
    # Justification : territoire de base, référencé par Service
    cur.execute("SELECT code_dept, COALESCE(nom, code_dept) FROM departement")
    rows = [{"code": r[0], "nom": r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("Departement (105 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (n:Departement {code_dept: r.code}) "
            "SET n.nom = r.nom",
            rows)

    # ── TypeFait ───────────────────────────────────────
    # Justification : entité terminale, référencée par ENREGISTRE
    cur.execute("SELECT code_index, libelle FROM type_fait ORDER BY code_index")
    rows = [{"code": r[0], "libelle": r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("TypeFait (103 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (n:TypeFait {code_index: r.code}) "
            "SET n.libelle = r.libelle",
            rows)

    # ── Service ────────────────────────────────────────
    # Justification : chargé en dernier car référence Force et Direction
    # via id_force et id_direction (pour les relations en Phase 2)
    cur.execute("""
        SELECT s.id_service, s.nom, s.code_dept,
               f.code AS force_code,
               COALESCE(d.code, NULL) AS dir_code
        FROM service s
        JOIN force f ON f.id_force = s.id_force
        LEFT JOIN direction d ON d.id_direction = s.id_direction
        ORDER BY s.id_service
    """)
    rows = [{"id": r[0], "nom": r[1], "dept": r[2],
             "force": r[3], "dir": r[4]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono(f"Service ({len(rows)} noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (n:Service {id_service: r.id}) "
            "SET n.nom        = r.nom, "
            "    n.code_dept  = r.dept, "
            "    n.force_code = r.force, "
            "    n.dir_code   = r.dir",
            rows)

    cur.close()

# ─────────────────────────────────────────────────────────
# PHASE 2 — Chargement des relations
# Justification : on MATCH les noeuds existants et on
# CREATE les relations — pas de doublon possible car
# les noeuds ont déjà été créés avec MERGE en Phase 1.
# ─────────────────────────────────────────────────────────

def phase2_relations(driver, pg):
    print("\n[PHASE 2] Chargement des relations")
    cur = pg.cursor()

    # ── [:APPARTIENT_A] Service → Force ───────────────
    # Justification : remplace la FK id_force de la table service
    cur.execute("SELECT s.id_service, f.code FROM service s JOIN force f ON f.id_force=s.id_force")
    rows = [{"sid": r[0], "fcode": r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("[:APPARTIENT_A] Service->Force (1482)", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (svc:Service {id_service: r.sid}) "
            "MATCH (f:Force     {code: r.fcode}) "
            "MERGE (svc)-[:APPARTIENT_A]->(f)",
            rows)

    # ── [:DEPEND_DE] Service → Direction (PN) ─────────
    # Justification : remplace la FK id_direction de service (NULL pour GN)
    cur.execute("""
        SELECT s.id_service, d.code
        FROM service s JOIN direction d ON d.id_direction=s.id_direction
        WHERE s.id_direction IS NOT NULL
    """)
    rows = [{"sid": r[0], "dcode": r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono(f"[:DEPEND_DE] Service->Direction ({len(rows)} PN)", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (svc:Service   {id_service: r.sid}) "
            "MATCH (dir:Direction {code: r.dcode}) "
            "MERGE (svc)-[:DEPEND_DE]->(dir)",
            rows)

    # ── [:SITUE_DANS] Service → Departement ───────────
    # Justification : remplace la FK code_dept de service
    cur.execute("SELECT id_service, code_dept FROM service")
    rows = [{"sid": r[0], "dept": r[1]} for r in cur.fetchall()]
    with driver.session() as s:
        chrono("[:SITUE_DANS] Service->Departement (1482)", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (svc:Service    {id_service: r.sid}) "
            "MATCH (dep:Departement{code_dept:  r.dept}) "
            "MERGE (svc)-[:SITUE_DANS]->(dep)",
            rows)

    # ── [:ENREGISTRE] Service → TypeFait ──────────────
    # Justification : remplace la table statistique entière.
    # Les propriétés {annee, nombre} portent les données
    # auparavant stockées en lignes séparées.
    # Chargé par lots de BATCH_SIZE car 588 553 relations.
    cur.execute("""
        SELECT id_service, annee, code_index, nombre
        FROM statistique
        ORDER BY id_service, annee, code_index
    """)
    all_stats = cur.fetchall()
    rows = [{"sid": r[0], "annee": r[1], "code": r[2], "nb": r[3]}
            for r in all_stats]

    total = len(rows)
    print(f"  [...] [:ENREGISTRE] {total:,} relations — chargement par lots de {BATCH_SIZE}...")
    t0 = time.perf_counter()
    with driver.session() as s:
        loaded = 0
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            s.run(
                "UNWIND $rows AS r "
                "MATCH (svc:Service  {id_service: r.sid}) "
                "MATCH (tf:TypeFait  {code_index: r.code}) "
                "MERGE (svc)-[e:ENREGISTRE {annee: r.annee}]->(tf) "
                "SET e.nombre = r.nb",
                rows=batch
            )
            loaded += len(batch)
            pct = loaded * 100 // total
            print(f"  \r  {pct:3d}%  {loaded:>7,}/{total:,}", end="", flush=True)
    dt = time.perf_counter() - t0
    print(f"\n  [OK] [:ENREGISTRE] {total:,} relations chargées en {dt:.1f}s")

    cur.close()

# ─────────────────────────────────────────────────────────
# PHASE 3 — Enrichissement
# Justification : données publiques (INSEE, géographie)
# impossibles à modéliser proprement en relationnel sans
# modifier le schéma existant.
# ─────────────────────────────────────────────────────────

# Régions françaises + départements (source : INSEE)
REGIONS = {
    "ARA": ("Auvergne-Rhône-Alpes",
            ["01","03","07","15","26","38","42","43","63","69","73","74"]),
    "BFC": ("Bourgogne-Franche-Comté",
            ["21","25","39","58","70","71","89","90"]),
    "BRE": ("Bretagne",
            ["22","29","35","56"]),
    "CVL": ("Centre-Val de Loire",
            ["18","28","36","37","41","45"]),
    "COR": ("Corse",
            ["2A","2B"]),
    "GES": ("Grand Est",
            ["08","10","51","52","54","55","57","67","68","88"]),
    "HDF": ("Hauts-de-France",
            ["02","59","60","62","80"]),
    "IDF": ("Ile-de-France",
            ["75","77","78","91","92","93","94","95"]),
    "NOR": ("Normandie",
            ["14","27","50","61","76"]),
    "NAQ": ("Nouvelle-Aquitaine",
            ["16","17","19","23","24","33","40","47","64","79","86","87"]),
    "OCC": ("Occitanie",
            ["09","11","12","30","31","32","34","46","48","65","66","81","82"]),
    "PDL": ("Pays de la Loire",
            ["44","49","53","72","85"]),
    "PAC": ("Provence-Alpes-Côte d'Azur",
            ["04","05","06","13","83","84"]),
    "GUA": ("Guadeloupe",   ["971"]),
    "MTQ": ("Martinique",   ["972"]),
    "GUY": ("Guyane",       ["973"]),
    "REU": ("La Réunion",   ["974"]),
    "MAY": ("Mayotte",      ["976"]),
}

# Catégories de faits (codes index)
CATEGORIES = [
    ("ATT_PERS",  "Atteintes aux personnes",             1,  14),
    ("VOLS",      "Vols et cambriolages",                15,  43),
    ("INF_SEX",   "Infractions sexuelles",               44,  50),
    ("ATT_MIN",   "Atteintes aux mineurs et famille",    51,  54),
    ("STUP",      "Stupéfiants et santé publique",       55,  61),
    ("DESTR",     "Destructions et dégradations",        62,  68),
    ("ETR",       "Infractions aux étrangers",           69,  71),
    ("ORD_PUB",   "Ordre public et environnement",       72,  80),
    ("FRAUDES",   "Fraudes, faux et escroqueries",       81,  95),
    ("ECO",       "Délits économiques",                  96, 107),
]

# Adjacences entre départements métropolitains
# Source : géographie France métropolitaine
ADJACENCES = [
    ("01","39"),("01","71"),("01","69"),("01","73"),("01","74"),("01","38"),("01","42"),
    ("02","08"),("02","51"),("02","60"),("02","77"),("02","80"),("02","59"),("02","02"),
    ("03","18"),("03","23"),("03","42"),("03","43"),("03","58"),("03","63"),("03","71"),
    ("04","05"),("04","06"),("04","13"),("04","26"),("04","83"),("04","84"),
    ("05","04"),("05","06"),("05","26"),("05","38"),("05","73"),
    ("06","04"),("06","05"),("06","83"),
    ("07","26"),("07","30"),("07","38"),("07","42"),("07","43"),("07","48"),("07","63"),
    ("08","02"),("08","51"),("08","55"),("08","59"),
    ("09","11"),("09","12"),("09","31"),("09","65"),("09","66"),
    ("10","21"),("10","51"),("10","52"),("10","77"),("10","89"),
    ("11","09"),("11","12"),("11","30"),("11","31"),("11","34"),("11","66"),
    ("12","09"),("12","11"),("12","15"),("12","30"),("12","31"),("12","34"),("12","46"),("12","48"),("12","81"),("12","82"),
    ("13","04"),("13","30"),("13","83"),("13","84"),
    ("14","27"),("14","50"),("14","61"),("14","76"),
    ("15","12"),("15","19"),("15","23"),("15","43"),("15","48"),("15","63"),
    ("16","17"),("16","19"),("16","24"),("16","33"),("16","79"),("16","86"),("16","87"),
    ("17","16"),("17","24"),("17","33"),("17","79"),("17","85"),
    ("18","03"),("18","23"),("18","28"),("18","36"),("18","41"),("18","45"),("18","58"),("18","89"),
    ("19","15"),("19","16"),("19","23"),("19","24"),("19","46"),("19","63"),("19","87"),
    ("21","10"),("21","25"),("21","39"),("21","52"),("21","58"),("21","70"),("21","71"),("21","89"),
    ("22","29"),("22","35"),("22","56"),
    ("23","03"),("23","15"),("23","16"),("23","18"),("23","19"),("23","36"),("23","63"),("23","87"),
    ("24","16"),("24","17"),("24","19"),("24","33"),("24","40"),("24","46"),("24","47"),("24","87"),
    ("25","21"),("25","39"),("25","70"),("25","90"),
    ("26","04"),("26","05"),("26","07"),("26","38"),("26","84"),
    ("27","14"),("27","28"),("27","60"),("27","61"),("27","76"),("27","78"),
    ("28","18"),("28","27"),("28","41"),("28","45"),("28","61"),("28","72"),("28","78"),
    ("29","22"),("29","56"),
    ("30","07"),("30","11"),("30","12"),("30","13"),("30","34"),("30","48"),("30","84"),
    ("31","09"),("31","11"),("31","32"),("31","33"),("31","34"),("31","40"),("31","47"),("31","65"),("31","81"),("31","82"),
    ("32","31"),("32","33"),("32","40"),("32","47"),("32","64"),("32","65"),("32","82"),
    ("33","16"),("33","17"),("33","24"),("33","32"),("33","40"),("33","47"),
    ("34","11"),("34","12"),("34","30"),("34","48"),("34","81"),
    ("35","22"),("35","44"),("35","50"),("35","53"),("35","56"),("35","61"),("35","72"),
    ("36","18"),("36","23"),("36","37"),("36","41"),("36","86"),("36","87"),
    ("37","28"),("37","36"),("37","41"),("37","49"),("37","72"),("37","79"),("37","86"),
    ("38","01"),("38","05"),("38","26"),("38","69"),("38","73"),
    ("39","01"),("39","21"),("39","25"),("39","70"),("39","71"),
    ("40","24"),("40","32"),("40","33"),("40","47"),("40","64"),
    ("41","18"),("41","28"),("41","36"),("41","37"),("41","45"),("41","72"),
    ("42","01"),("42","03"),("42","07"),("42","38"),("42","43"),("42","63"),("42","69"),
    ("43","03"),("43","07"),("43","12"),("43","15"),("43","42"),("43","48"),("43","63"),
    ("44","35"),("44","49"),("44","56"),("44","85"),
    ("45","18"),("45","28"),("45","41"),("45","58"),("45","77"),("45","89"),
    ("46","12"),("46","15"),("46","19"),("46","24"),("46","47"),("46","82"),
    ("47","24"),("47","31"),("47","32"),("47","33"),("47","40"),("47","46"),("47","82"),
    ("48","07"),("48","12"),("48","15"),("48","30"),("48","34"),("48","43"),
    ("49","35"),("49","37"),("49","44"),("49","53"),("49","72"),("49","79"),("49","85"),("49","86"),
    ("50","14"),("50","35"),("50","61"),
    ("51","02"),("51","08"),("51","10"),("51","52"),("51","55"),("51","77"),
    ("52","10"),("52","21"),("52","51"),("52","55"),("52","67"),("52","68"),("52","70"),("52","88"),
    ("53","35"),("53","49"),("53","61"),("53","72"),
    ("54","55"),("54","57"),("54","67"),("54","68"),("54","88"),("54","52"),
    ("55","08"),("55","51"),("55","52"),("55","54"),("55","57"),("55","88"),
    ("56","22"),("56","29"),("56","35"),("56","44"),
    ("57","54"),("57","55"),("57","67"),("57","88"),
    ("58","03"),("58","18"),("58","21"),("58","45"),("58","71"),("58","89"),
    ("59","02"),("59","08"),("59","62"),("59","80"),
    ("60","02"),("60","27"),("60","76"),("60","77"),("60","80"),("60","95"),
    ("61","14"),("61","27"),("61","28"),("61","35"),("61","50"),("61","53"),("61","72"),("61","76"),
    ("62","59"),("62","80"),
    ("63","03"),("63","07"),("63","15"),("63","19"),("63","23"),("63","42"),("63","43"),
    ("64","32"),("64","40"),("64","65"),
    ("65","09"),("65","31"),("65","32"),("65","64"),("65","66"),
    ("66","09"),("66","11"),("66","65"),
    ("67","54"),("67","52"),("67","57"),("67","68"),("67","88"),("67","54"),
    ("68","25"),("68","52"),("68","54"),("68","67"),("68","70"),("68","90"),
    ("69","01"),("69","38"),("69","42"),("69","71"),
    ("70","21"),("70","25"),("70","39"),("70","52"),("70","68"),("70","88"),("70","90"),
    ("71","01"),("71","03"),("71","21"),("71","39"),("71","42"),("71","58"),("71","69"),
    ("72","28"),("72","35"),("72","37"),("72","41"),("72","49"),("72","53"),("72","61"),
    ("73","01"),("73","05"),("73","38"),("73","74"),
    ("74","01"),("74","73"),
    ("75","77"),("75","78"),("75","91"),("75","92"),("75","93"),("75","94"),("75","95"),
    ("76","14"),("76","27"),("76","60"),("76","61"),("76","80"),
    ("77","02"),("77","10"),("77","45"),("77","51"),("77","60"),("77","75"),("77","78"),("77","89"),("77","91"),
    ("78","27"),("78","28"),("78","75"),("78","77"),("78","91"),("78","92"),("78","95"),
    ("79","16"),("79","17"),("79","37"),("79","49"),("79","85"),("79","86"),
    ("80","02"),("80","59"),("80","60"),("80","62"),("80","76"),
    ("81","12"),("81","30"),("81","31"),("81","34"),("81","82"),
    ("82","12"),("82","31"),("82","32"),("82","46"),("82","47"),("82","81"),
    ("83","04"),("83","06"),("83","13"),
    ("84","04"),("84","07"),("84","13"),("84","26"),("84","30"),
    ("85","17"),("85","44"),("85","49"),("85","79"),
    ("86","16"),("86","36"),("86","37"),("86","49"),("86","79"),("86","87"),
    ("87","16"),("87","19"),("87","23"),("87","24"),("87","36"),("87","86"),
    ("88","52"),("88","54"),("88","55"),("88","57"),("88","67"),("88","68"),("88","70"),
    ("89","10"),("89","18"),("89","21"),("89","45"),("89","58"),("89","77"),
    ("90","25"),("90","68"),("90","70"),
    ("91","45"),("91","75"),("91","77"),("91","78"),("91","92"),("91","94"),
    ("92","75"),("92","78"),("92","91"),("92","93"),("92","94"),("92","95"),
    ("93","75"),("93","77"),("93","92"),("93","94"),("93","95"),
    ("94","75"),("94","77"),("94","91"),("94","92"),("94","93"),
    ("95","27"),("95","60"),("95","75"),("95","77"),("95","78"),("95","92"),("95","93"),
]

def phase3_enrichissement(driver, pg):
    print("\n[PHASE 3] Enrichissement")
    cur = pg.cursor()

    # ── Noeuds Region ─────────────────────────────────
    # Justification : les 18 régions permettent l'analyse
    # territoriale sans requêtes SQL complexes sur plusieurs tables.
    region_rows = [{"code": code, "nom": nom, "depts": depts}
                   for code, (nom, depts) in REGIONS.items()]
    with driver.session() as s:
        chrono("Region (18 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (reg:Region {code_region: r.code}) "
            "SET reg.nom = r.nom",
            region_rows)

    # ── [:SITUE_DANS] Departement → Region ────────────
    # Justification : permet de remonter de département en région
    # en une seule traversée de relation.
    cur.execute("SELECT code_dept FROM departement")
    existing_depts = {r[0] for r in cur.fetchall()}
    rel_rows = []
    for code_region, (nom, depts) in REGIONS.items():
        for dept in depts:
            if dept in existing_depts:
                rel_rows.append({"dept": dept, "region": code_region})
    with driver.session() as s:
        chrono(f"[:SITUE_DANS] Departement->Region ({len(rel_rows)})", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (d:Departement {code_dept:  r.dept}) "
            "MATCH (reg:Region    {code_region: r.region}) "
            "MERGE (d)-[:SITUE_DANS]->(reg)",
            rel_rows)

    # ── Noeuds Categorie ──────────────────────────────
    # Justification : remplace les CASE WHEN dans les requêtes SQL.
    # Chaque TypeFait est relié à sa catégorie en une seule relation.
    cat_rows = [{"code": c, "nom": n, "plage": f"{s}-{e}"}
                for c, n, s, e in CATEGORIES]
    with driver.session() as s:
        chrono("Categorie (10 noeuds)", run_batch, s,
            "UNWIND $rows AS r "
            "MERGE (c:Categorie {code: r.code}) "
            "SET c.nom = r.nom, c.plage_codes = r.plage",
            cat_rows)

    # ── [:APPARTIENT_A] TypeFait → Categorie ──────────
    # Justification : permet d'agréger par famille de crimes
    # sans CASE WHEN ni table de correspondance externe.
    cur.execute("SELECT code_index FROM type_fait")
    all_codes = [r[0] for r in cur.fetchall()]
    tf_cat_rows = []
    for ci in all_codes:
        for code_cat, _, start, end in CATEGORIES:
            if start <= ci <= end:
                tf_cat_rows.append({"ci": ci, "cat": code_cat})
                break
    with driver.session() as s:
        chrono(f"[:APPARTIENT_A] TypeFait->Categorie ({len(tf_cat_rows)})", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (tf:TypeFait  {code_index: r.ci}) "
            "MATCH (c:Categorie  {code: r.cat}) "
            "MERGE (tf)-[:APPARTIENT_A]->(c)",
            tf_cat_rows)

    # ── [:ADJACENT_A] Departement ↔ Departement ───────
    # Justification : relation impossible en SQL sans table externe.
    # En graphe elle devient native et permet des requêtes de
    # propagation et de voisinage en quelques lignes de Cypher.
    adj_rows = [{"a": a, "b": b} for a, b in ADJACENCES
                if a in existing_depts and b in existing_depts]
    with driver.session() as s:
        chrono(f"[:ADJACENT_A] Adjacences ({len(adj_rows)} paires)", run_batch, s,
            "UNWIND $rows AS r "
            "MATCH (da:Departement {code_dept: r.a}) "
            "MATCH (db:Departement {code_dept: r.b}) "
            "MERGE (da)-[:ADJACENT_A]-(db)",
            adj_rows)

    cur.close()

# ─────────────────────────────────────────────────────────
# RÉSUMÉ
# ─────────────────────────────────────────────────────────

def print_resume(driver):
    print("\n=== RÉSUMÉ DU GRAPHE NEO4J ===")
    with driver.session() as s:
        labels = ["Force","Direction","Departement","Service",
                  "TypeFait","Region","Categorie"]
        for lbl in labels:
            n = s.run(f"MATCH (n:{lbl}) RETURN count(n) AS c").single()["c"]
            print(f"  :{lbl:<15s} {n:>6,} noeuds")

        print()
        rels = ["APPARTIENT_A","DEPEND_DE","SITUE_DANS","ENREGISTRE","ADJACENT_A"]
        for rel in rels:
            n = s.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
            print(f"  [:{rel:<15s}]  {n:>8,} relations")

# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PHASE 3 - Migration PostgreSQL -> Neo4j")
    print("=" * 60)

    # Connexions
    pg = psycopg2.connect(**PG)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print("[OK] Connexion Neo4j établie")
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter à Neo4j : {e}")
        print("  → Vérifie que Neo4j Desktop est démarré (bouton Start)")
        pg.close()
        return

    t_global = time.perf_counter()

    # Nettoyage complet si relance
    print("\n[INFO] Nettoyage de la base Neo4j...")
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    print("[OK] Base vidée")

    phase0_contraintes(driver)
    phase1_noeuds(driver, pg)
    phase2_relations(driver, pg)
    phase3_enrichissement(driver, pg)

    dt = time.perf_counter() - t_global
    print(f"\n[DONE] Migration complète en {dt:.1f}s")
    print_resume(driver)

    pg.close()
    driver.close()

if __name__ == "__main__":
    main()
