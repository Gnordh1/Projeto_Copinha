from playwright.sync_api import sync_playwright
import pandas as pd
import json
import sqlite3
from datetime import datetime

TOURNAMENT_ID = 10772
SEASON_ID = 87614

def calcular_idade(timestamp):
    if not timestamp:
        return "N/A"  # Se não houver dado, marca como N/A
    try:
        # Se o timestamp for muito grande (milisegundos), converte para segundos
        if timestamp > 10000000000: 
            timestamp = timestamp / 1000
            
        nascimento = datetime.fromtimestamp(timestamp)
        hoje = datetime(2026, 1, 5) # Data de referência da Copinha
        
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception as e:
        print(f"Erro ao calcular idade: {e}")
        return "Erro"

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
                        jogos.append({
                            'id': str(e['id']),
                            'home': e['homeTeam']['name'],
                            'away': e['awayTeam']['name']
                        })
        except: pass
        browser.close()
    return jogos

def extrair_com_idade():
    jogos = buscar_ids_e_nomes()
    print(f"🚀 Iniciando extração de {len(jogos)} jogos com cálculo de idade...")
    
    conn = sqlite3.connect('copinha_scout_estruturado.db')
    primeira_insercao = True

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        for i, jogo in enumerate(jogos):
            page = context.new_page()
            try:
                page.goto(f"https://api.sofascore.com/api/v1/event/{jogo['id']}/lineups", timeout=60000)
                data = json.loads(page.locator("body").inner_text())
                
                if 'home' not in data: continue

                lista_jogadores = []
                for lado in ['home', 'away']:
                    for j in data[lado].get('players', []):
                        stats_brutas = j.get('statistics', {})
                        if not stats_brutas: continue
                        
                        # Limpeza de dicionários para não travar o SQL
                        stats_limpas = {k: v for k, v in stats_brutas.items() if not isinstance(v, dict)}
                        
                        p_info = j.get('player', {})
                        
                        # --- CAPTURA E CÁLCULO DA IDADE ---
                        timestamp = p_info.get('dateOfBirthTimestamp')
                        idade = calcular_idade(timestamp)
                        
                        linha = {
                            'match_id': jogo['id'],
                            'time': jogo[lado],
                            'nome': p_info.get('name'),
                            'posicao': j.get('position') or p_info.get('position', 'N/A'),
                            'idade': idade, # Aqui entra a nova coluna
                            **stats_limpas
                        }
                        lista_jogadores.append(linha)

                if lista_jogadores:
                    df = pd.DataFrame(lista_jogadores)
                    if primeira_insercao:
                        df.to_sql('scouts', conn, if_exists='replace', index=False)
                        primeira_insercao = False
                    else:
                        # Garante que a coluna 'idade' e outras novas existam no banco
                        df_db = pd.read_sql("SELECT * FROM scouts LIMIT 1", conn)
                        for col in df.columns:
                            if col not in df_db.columns:
                                conn.execute(f'ALTER TABLE scouts ADD COLUMN "{col}"')
                        df.to_sql('scouts', conn, if_exists='append', index=False)
                    
                    print(f"[{i+1}/{len(jogos)}] ✅ {jogo['home']} x {jogo['away']}")
            except: continue
            finally: page.close()
        
        browser.close()
    conn.close()

if __name__ == "__main__":
    extrair_com_idade()