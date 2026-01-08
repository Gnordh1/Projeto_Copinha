from playwright.sync_api import sync_playwright
import pandas as pd
import json
import sqlite3
from datetime import datetime

TOURNAMENT_ID = 10772
SEASON_ID = 87614

def calcular_idade(timestamp):
    if not timestamp: return "N/A"
    try:
        if timestamp > 10000000000: timestamp = timestamp / 1000
        nascimento = datetime.fromtimestamp(timestamp)
        hoje = datetime(2026, 1, 5) 
        return hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
    except: return "Erro"

def buscar_ids_e_nomes():
    jogos = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            for r in range(1, 4):
                url = f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/season/{SEASON_ID}/events/round/{r}"
                page.goto(url, wait_until="networkidle")
                data = json.loads(page.locator("body").inner_text())
                for e in data.get('events', []):
                    if e.get('status', {}).get('type') == 'finished':
                        jogos.append({'id': str(e['id']), 'home': e['homeTeam']['name'], 'away': e['awayTeam']['name']})
        except: pass
        browser.close()
    return jogos

def extrair_consolidado():
    jogos = buscar_ids_e_nomes()
    print(f"🚀 Extraindo e unificando {len(jogos)} jogos...")
    
    lista_bruta = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        for i, jogo in enumerate(jogos):
            page = context.new_page()
            try:
                page.goto(f"https://api.sofascore.com/api/v1/event/{jogo['id']}/lineups", timeout=60000)
                data = json.loads(page.locator("body").inner_text())
                for lado in ['home', 'away']:
                    for j in data[lado].get('players', []):
                        stats = j.get('statistics', {})
                        if not stats: continue
                        p_info = j.get('player', {})
                        
                        linha = {
                            'nome': p_info.get('name'),
                            'time': jogo[lado],
                            'posicao': j.get('position') or p_info.get('position', 'N/A'),
                            'idade': calcular_idade(p_info.get('dateOfBirthTimestamp')),
                            'matches': 1, # ADICIONADO: Cada linha de lineup conta como 1 jogo
                            **{k: v for k, v in stats.items() if not isinstance(v, dict)}
                        }
                        lista_bruta.append(linha)
                print(f"[{i+1}/{len(jogos)}] ✅ {jogo['home']} x {jogo['away']}")
            except: continue
            finally: page.close()
        browser.close()

    if lista_bruta:
        df = pd.DataFrame(lista_bruta)
        
        # 1. DEFINIR POSIÇÃO POR MINUTOS JOGADOS
        df_posicao = df.groupby(['nome', 'time', 'posicao'])['minutesPlayed'].sum().reset_index()
        df_posicao_principal = df_posicao.sort_values('minutesPlayed', ascending=False).drop_duplicates(['nome', 'time'])
        df_posicao_principal = df_posicao_principal[['nome', 'time', 'posicao']].rename(columns={'posicao': 'posicao_final'})

        # 2. UNIFICAR OS DADOS
        df = df.merge(df_posicao_principal, on=['nome', 'time'])

        # 3. REGRAS DE AGREGAÇÃO
        cols_metadados = ['nome', 'time', 'posicao', 'posicao_final', 'idade', 'match_id']
        agg_rules = {col: 'sum' for col in df.columns if col not in cols_metadados}
        
        # Aplicar médias
        for col_avg in ['rating', 'expectedGoals', 'expectedAssists']:
            if col_avg in agg_rules: agg_rules[col_avg] = 'mean'

        df_final = df.groupby(['nome', 'time', 'posicao_final', 'idade']).agg(agg_rules).reset_index()
        df_final = df_final.rename(columns={'posicao_final': 'posicao'})
        df_final = df_final.round(2)

        cols = list(df_final.columns)
        if 'matches' in cols:
            cols.insert(4, cols.pop(cols.index('matches')))
            df_final = df_final[cols]

        conn = sqlite3.connect('copinha_scout_estruturado.db')
        df_final.to_sql('scouts', conn, if_exists='replace', index=False)
        conn.close()
        print(f"💾 Finalizado! {len(df_final)} atletas únicos no banco com contagem de jogos.")

if __name__ == "__main__":
    extrair_consolidado()