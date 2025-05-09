import psycopg2
from datetime import datetime
import csv
import uuid
from objectiveClass import Objective
from resultClass import Result
from iniciativeClass import Iniciative
from aclClass import Acl

conn = psycopg2.connect(
    dbname="db_fiec",
    user="postgres",
    password="oigente123",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

csvAreasNaoImportadas = open('areas_nao_importadas.csv', 'w')
csvWriterAreas = csv.writer(csvAreasNaoImportadas, delimiter=',')
csvUsersNaoImportadas = open('usuarios_nao_importadas.csv', 'w')
csvWriterUsers = csv.writer(csvUsersNaoImportadas, delimiter=',')

# Função segura para converter strings em inteiros
def parse_int(val):
    try:
        return int(str(val).replace("%", "").replace(".", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return 0
    
def getUser(email):
    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
    user_result = cur.fetchone()
    if not user_result:
        return 0
    return user_result[0]

def getArea(area):
    cur.execute("SELECT id FROM user_areas WHERE area = %s", (area,))
    area = cur.fetchone()
    if not area:
        return 0
    return area[0]

def addResponsible(user_id, mandala_id, result_id, metric_id):
    cur.execute("""SELECT id FROM responsibles WHERE mandala_id = %s AND user_id = %s""", (mandala_id, user_id,))
    resp = cur.fetchone()
    if not resp:
        cur.execute("""INSERT INTO responsibles (user_id, mandala_id, result_id, metric_id, rule) 
        VALUES (%s, %s, %s, %s, %s) 
        RETURNING id""", (user_id, mandala_id, result_id, metric_id, 2))
        resp = cur.fetchone()

    return resp[0]

def addUserArea(resp_id, area_id):
    cur.execute("""INSERT INTO responsible_user_areas (responsible_id, user_area_id, created, modified) 
        VALUES (%s, %s, %s, %s) 
        RETURNING id""", (
            resp_id,
            area_id,
            datetime.now(),
            datetime.now(),
        ))
    
def treatPeriods(metric_id, metric):
    cur.execute("SELECT * FROM metric_periods WHERE metric_id = %s ORDER BY id ASC", (metric_id,))
    periods = cur.fetchall()
    for period in periods:
        srtPeriod = 'meta' + str(period[1])
        cur.execute("""UPDATE metric_periods SET expected = %s WHERE id = %s""", (metric[srtPeriod], period[0]))

def save_project_to_postgres(projeto, objetivos, resultados, iniciativas):

    # Buscar ID do proprietário
    proprietario_id = getUser(projeto['proprietario'])
    if proprietario_id == 0:
        proprietario_id = 1

    # Inserir mandala
    cur.execute("""
        INSERT INTO mandalas (title, center_color, owner_id, description, created, modified, public, total, total_quarters)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        "FIEC 2025 V4",
        'rgb(255,255,255)',
        proprietario_id,
        projeto.get('description', ''),
        datetime.now(),
        datetime.now(),
        False,
        0,
        0
    ))
    mandala_id = cur.fetchone()[0]

    # Objetivos
    goal_id_map = {}
    for obj in objetivos:
        # print(obj)
        # exit()
        cur.execute("""
            INSERT INTO goals (title, alias, slice_color, mandala_id, created, modified, initial_date, expire_date, owner_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            obj['nome'],
            obj['sigla'],
            'rgb(255,255,255)',
            mandala_id,
            datetime.now(),
            datetime.now(),
            obj.get("dataInicial", None),
            obj.get("dataVencimento", None),
            proprietario_id
        ))
        goal_id_map[obj['id_objetivo']] = cur.fetchone()[0]

    # Resultados
    result_id_map = {}
    for res in resultados:
        cur.execute("""
            INSERT INTO results (title, alias, slice_color, goal_id, created, modified, initial_date, expire_date, owner_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            res['nomeResultado'],
            res['sigla'],
            'rgb(255,255,255)',
            goal_id_map[res['idObjetivoPai']],
            datetime.now(),
            datetime.now(),
            obj.get("dataInicial", None),
            obj.get("dataVencimento", None),
            proprietario_id
        ))
        result_id_map[res['idResultado']] = cur.fetchone()[0]

    # Iniciativas
    ownersNaoEncontrados = []
    areasnNaoEncontradas = []
    for met in iniciativas:
        cur.execute("""
            INSERT INTO metrics (title, measure_unit, result_id, metric_type_id, total_periods, created, modified, owner_id)
            VALUES (%s, %s, %s, 1, 4, %s, %s, %s)
            RETURNING id
        """, (
            met['nomeMetrica'],
            met['unidadeMedida'],
            result_id_map[met['idResultado']],
            datetime.now(),
            datetime.now(),
            proprietario_id
        ))
        metric_id = cur.fetchone()[0]

        for indexOwners in range(1, len(met['donos']), 2):
            user_id = getUser(met['donos'][indexOwners])
            if (user_id == 0):
                ownersNaoEncontrados.append(met['donos'][indexOwners])
                if met['donos'][indexOwners] not in ownersNaoEncontrados:
                    csvWriterUsers.writerow([met['donos'][indexOwners]])
                continue
                
            area_id = getArea(met['donos'][indexOwners - 1])
            if (area_id == 0):
                areasnNaoEncontradas.append(met['donos'][indexOwners - 1])
                if met['donos'][indexOwners - 1] not in areasnNaoEncontradas:
                    csvWriterAreas.writerow([met['donos'][indexOwners - 1], met['donos'][indexOwners]])
                continue
                
            responsible_id = addResponsible(user_id, mandala_id, result_id_map[met['idResultado']], metric_id)
            addUserArea(responsible_id, area_id)
        
        treatPeriods(metric_id, met)

    conn.commit()
    cur.close()
    conn.close()
    return mandala_id


# Início da leitura do CSV e montagem da mandala
mandala = Acl(str(uuid.uuid4()), [])

with open('importar10.csv', 'r', encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    objectiveList = []
    lastObjective = None
    lastResult = None

    ownersAcl = []
    lastOwnerObjName = ''
    lastOwnerResName = ''
    ownersObj = []
    ownersRes = []
    ownersMet = []
    extensoesObj = []
    extensoesRes = []

    for line in reader:
        if not line or len(line) < 13:
            continue

        mandalaDict = mandala.asDict()
        totalObjectives = len(mandalaDict['objetivosPrincipais']) + 1

        # Objetivo
        if line[1] != '':
            sigla = str(line[1])
            sweepAngle = 360 / totalObjectives
            objective = Objective(str(uuid.uuid4()), line[3], sweepAngle, sigla, extensoesObj, ownersObj)
            objective = mandala.appendObjectives(objective.asDict())
            mandala.treatAnglesObjectives()

            if lastObjective and lastObjective['id_objetivo'] != objective['id_objetivo']:
                mandala.setOwnersToObjective(lastObjective, ownersObj[:])
                mandala.setExtensionsToObjective(lastObjective, extensoesObj[:])

            if line[13] != '':
                ownersObj.clear()

            if line[2]:
                extensoesObj.clear()

            lastObjective = objective

        # Resultado
        if line[4] != '' or line[6] != '':
            siglaKR = 'KR' + str(parse_int(line[4]))
            nomeKR = line[6]
            result = Result(str(uuid.uuid4()), objective['id_objetivo'], nomeKR, siglaKR, extensoesRes, ownersRes)
            result = mandala.appendResults(result.asDict())
            mandala.treatAnglesResults(objective)

            if lastResult and lastResult['idResultado'] != result['idResultado']:
                mandala.setOwnersToResult(lastResult, ownersRes[:])
                mandala.setExtensionsToResult(lastResult, extensoesRes[:])

            if line[13] != '':
                ownersRes.clear()

            if line[5]:
                extensoesRes.clear()

            lastResult = result

        # Donos
        ownersMet.clear()
        for indexOwners in range(6):
            if line[13 + indexOwners] != '':
                if line[13 + indexOwners].lower() not in ownersObj:
                    if line[0] != '':
                        lastOwnerObjName = line[0]
                    ownersObj.append(lastOwnerObjName)
                    ownersObj.append(line[13 + indexOwners].lower())

                if line[13 + indexOwners].lower() not in ownersRes:
                    if line[0] != '':
                        lastOwnerResName = line[0]
                    ownersRes.append(lastOwnerResName)
                    ownersMet.append(lastOwnerResName)
                    ownersRes.append(line[13 + indexOwners].lower())
                    ownersMet.append(line[13 + indexOwners].lower())

                if all(acl['identificador'] != line[13 + indexOwners].lower() for acl in ownersAcl):
                    ownersAcl.append({
                        'identificador': line[13 + indexOwners].lower(),
                        'permissao': 'responsavel'
                    })

        if line[2] != '':
            extensoesObj.append(line[2].lower())
        if line[5] != '':
            extensoesRes.append(line[5].lower())

        # Iniciativa
        unidadeMedida = line[8]
        meta1 = parse_int(line[9])
        meta2 = parse_int(line[10])
        meta3 = parse_int(line[11])
        meta4 = parse_int(line[12])

        iniciative = Iniciative(str(uuid.uuid4()), result['idResultado'], line[7], meta1, meta2, meta3, meta4, unidadeMedida, ownersMet[:])
        mandala.appendIniciatives(iniciative.asDict())

    mandala.setOwnersToObjective(lastObjective, ownersObj[:])
    mandala.setOwnersToResult(lastResult, ownersRes[:])
    mandala.setOwnersToACL(ownersAcl)
    mandala.setExtensionsToObjective(lastObjective, extensoesObj[:])
    mandala.setExtensionsToResult(lastResult, extensoesRes[:])

# Salva no PostgreSQL
mandala.treatAnglesObjectives()
mandala_dict = mandala.asDict()

save_project_to_postgres(
    projeto=mandala_dict,
    objetivos=mandala_dict['objetivosPrincipais'],
    resultados=mandala_dict['resultadosPrincipais'],
    iniciativas=mandala_dict['metricasPrincipais'],
)
