from playwright.sync_api import sync_playwright
import pandas as pd
import json
import sqlite3
from datetime import datetime

TOURNAMENT_ID = 10772
SEASON_ID = 87614

def calcular_idade(timestamp):
    if not timestamp:
        return "N/A"
    try:
        if timestamp > 10000000000: 
            timestamp = timestamp / 1000
        nascimento = datetime.fromtimestamp(timestamp)
        hoje = datetime(2026, 1, 5) 
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception as e:
        return "Erro"

def buscar_ids_e_nomes():
    jogos = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            # Pegando as 3 primeiras rodadas
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

def extrair_com_idade():
    jogos = buscar_ids_e_nomes()
    print(f"🚀 Iniciando extração e consolidação de {len(jogos)} jogos...")
    
    lista_rodada_atual = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        for i, jogo in enumerate(jogos):
            page = context.new_page()
            try:
                page.goto(f"https://api.sofascore.com/api/v1/event/{jogo['id']}/lineups", timeout=60000)
                data = json.loads(page.locator("body").inner_text())
                if 'home' not in data: continue

                for lado in ['home', 'away']:
                    for j in data[lado].get('players', []):
                        stats_brutas = j.get('statistics', {})
                        if not stats_brutas: continue
                        
                        stats_limpas = {k: v for k, v in stats_brutas.items() if not isinstance(v, dict)}
                        p_info = j.get('player', {})
                        idade = calcular_idade(p_info.get('dateOfBirthTimestamp'))
                        
                        linha = {
                            'nome': p_info.get('name'),
                            'time': jogo[lado],
                            'posicao': j.get('position') or p_info.get('position', 'N/A'),
                            'idade': idade,
                            **stats_limpas
                        }
                        lista_rodada_atual.append(linha)
                print(f"[{i+1}/{len(jogos)}] ✅ Lido: {jogo['home']} x {jogo['away']}")
            except: continue
            finally: page.close()
        browser.close()

    if lista_rodada_atual:
        df_novo = pd.DataFrame(lista_rodada_atual)
        
        # --- LÓGICA DE UNIFICAÇÃO (SOMA E MÉDIA) ---
        print("📊 Consolidando estatísticas dos jogadores...")
        
        agg_rules = {col: 'sum' for col in df_novo.columns if col not in ['nome', 'time', 'posicao', 'idade']}
        
        for col_media in ['rating', 'expectedGoals', 'expectedAssists']:
            if col_media in agg_rules:
                agg_rules[col_media] = 'mean'
        
        # Agrupamos por Nome e Time
        df_consolidado = df_novo.groupby(['nome', 'time', 'posicao', 'idade']).agg(agg_rules).reset_index()

        conn = sqlite3.connect('copinha_scout_estruturado.db')
        df_consolidado.to_sql('scouts', conn, if_exists='replace', index=False)
        conn.close()
        print(" Banco de Dados atualizado com sucesso!")

if __name__ == "__main__":
    extrair_com_idade()