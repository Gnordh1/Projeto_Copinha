from playwright.sync_api import sync_playwright
import pandas as pd
import json
import sqlite3
from datetime import datetime

TOURNAMENT_ID = 10772
SEASON_ID = 87614

def extrair_lista_jogos():
    jogos_info = []
    ids_vistos = set()

    print("🚀 Iniciando extração da lista de partidas via Playwright...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()       
        
        try:
            for r in [1, 2, 3]:
                url = f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/season/{SEASON_ID}/events/round/{r}"
                page.goto(url)
                try:
                    content = page.locator("body").inner_text()
                    data = json.loads(content)
                    for e in data.get('events', []):
                        e_id = str(e['id'])
                        if e_id not in ids_vistos:
                            # Captura dados da partida
                            info = {
                                'game_id': e_id,
                                'rodada': r,
                                'data_timestamp': e.get('startTimestamp'),
                                'home_team': e['homeTeam']['name'],
                                'away_team': e['awayTeam']['name'],
                                'home_score': e.get('homeScore', {}).get('display', 0),
                                'away_score': e.get('awayScore', {}).get('display', 0),
                            }
                            jogos_info.append(info)
                            ids_vistos.add(e_id)
                except: continue

            for bloco in range(0, 3): 
                url_bloco = f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/season/{SEASON_ID}/events/last/{bloco}"
                page.goto(url_bloco)
                try:
                    content = page.locator("body").inner_text()
                    data = json.loads(content)
                    for e in data.get('events', []):
                        e_id = str(e['id'])
                        if e_id not in ids_vistos:
                            info = {
                                'game_id': e_id,
                                'rodada': e.get('roundInfo', {}).get('round', 'N/A'),
                                'data_timestamp': e.get('startTimestamp'),
                                'home_team': e['homeTeam']['name'],
                                'away_team': e['awayTeam']['name'],
                                'home_score': e.get('homeScore', {}).get('display', 0),
                                'away_score': e.get('awayScore', {}).get('display', 0),
                            }
                            jogos_info.append(info)
                            ids_vistos.add(e_id)
                except: break

        except Exception as e:
            print(f"❌ Erro durante a navegação: {e}")
        finally:
            browser.close()

    if jogos_info:
        df = pd.DataFrame(jogos_info)
        
        # Converter timestamp para data legível
        df['data_hora'] = pd.to_datetime(df['data_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo').dt.strftime('%d/%m/%Y %H:%M')

        conn = sqlite3.connect('copinha_scout_estruturado.db')
        df.to_sql('partidas', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"💾 Sucesso! {len(df)} partidas salvas na tabela 'partidas'.")
        return df
    else:
        print("⚠️ Nenhum jogo encontrado.")
        return None

if __name__ == "__main__":
    extrair_lista_jogos()