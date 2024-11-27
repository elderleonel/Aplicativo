import glob
import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import itertools
import time
from datetime import datetime, timedelta
import logging
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

# Configuração do log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variáveis globais para controlar o processamento
processando = False
pausado = False
progress_data = {}
engrenagem = itertools.cycle(["|", "/", "-", "\\"])
arquivos_processados = {}
diretorio_saida = ""
input_file = ""

# Lista de códigos de cidades (exemplo)
cidades = {
    '9177': 'Mirassol D\'Oeste',
    '9121': 'Nossa Senhora do Livramento',
    '8993': 'São José dos Quatro Marcos',
}

# Dicionário para controlar o status de cada cidade
cidade_status = {}

# Funções auxiliares
def atualizar_engrenagem():
    if processando:
        char = next(engrenagem)
        gear_label.config(text=char)
        app.after(250, atualizar_engrenagem)  # Atualiza a cada 250ms

def calcular_estimativa(total_lines, processed_lines, elapsed_time):
    try:
        if processed_lines == 0:
            return datetime.now() + timedelta(days=1)  # Estimativa inicial arbitrária
        estimated_total_time = elapsed_time * (total_lines / processed_lines)
        estimated_end_time = datetime.now() + timedelta(seconds=estimated_total_time - elapsed_time)
        return estimated_end_time
    except Exception as e:
        logging.error(f"Erro ao calcular estimativa: {e}")
        return datetime.now() + timedelta(days=1)

def formatar_tempo(estimated_end_time):
    remaining_time = estimated_end_time - datetime.now()
    total_seconds = int(remaining_time.total_seconds())
    if total_seconds < 0:
        return "Tempo inválido"
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 365:
        return "Mais de um ano"
    elif days > 0:
        return f"{days}d {hours}h {minutes}min"
    elif hours > 0:
        return f"{hours}h {minutes}min"
    else:
        return f"{minutes}min {seconds:.0f}s"

def processar_chunk(chunk, campo):
    try:
        bloco = []
        dados_filtrados = []
        for line in chunk:
            if line.startswith('01000'):
                bloco = [line]
            elif line.startswith('99999'):
                bloco.append(line)
                if any(f'|{campo}|' in registro for registro in bloco):
                    dados_filtrados.extend(bloco)
            else:
                bloco.append(line)
        return dados_filtrados
    except Exception as e:
        logging.error(f"Erro no processamento do chunk: {e}")
        return []

def filtrar_dados(input_file, campo, progress_label, progress_bar, app, start_time, total_lines):
    global processando, pausado, progress_data, arquivos_processados
    logging.info(f"Iniciando filtragem para {cidades[campo]}")

    # Atualizando o status da cidade na interface
    cidade_status[campo] = "Processando"
    atualizar_status_cidades()

    try:
        chunk_size = 100000
        dados_filtrados = []
        processed_lines = progress_data.get("processed_lines", 0)
        line_queue = Queue()

        def leitor():
            with open(input_file, 'r') as infile:
                while True:
                    lines = [infile.readline() for _ in range(chunk_size)]
                    if not lines or not lines[0]:
                        break
                    line_queue.put(lines)

        reader_thread = threading.Thread(target=leitor)
        reader_thread.start()

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            while reader_thread.is_alive() or not line_queue.empty():
                if not processando:
                    progress_label.config(text="Processamento abortado.")
                    app.update_idletasks()
                    return []

                while pausado:
                    time.sleep(0.1)
                    app.update_idletasks()

                if not line_queue.empty():
                    chunk = line_queue.get()
                    future = executor.submit(processar_chunk, chunk, campo)
                    try:
                        dados_filtrados.extend(future.result())
                    except Exception as e:
                        logging.error(f"Erro ao processar chunk: {e}")

                    processed_lines += len(chunk)

                    progress = (processed_lines / total_lines) * 100
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    estimated_end_time = calcular_estimativa(total_lines, processed_lines, elapsed_time)
                    tempo_restante_formatado = formatar_tempo(estimated_end_time)

                    progress_label.config(
                        text=f"Processando: {progress:.2f}% concluído\nTempo estimado: {tempo_restante_formatado}")
                    progress_bar['value'] = progress

                    # Atualizar as informações da cidade e a quantidade de registros
                    if campo not in arquivos_processados:
                        arquivos_processados[campo] = 0
                    arquivos_processados[campo] = len(dados_filtrados)

                    cidade_progresso_label.config(
                        text=f"Cidade: {cidades[campo]}\nRegistros Encontrados: {arquivos_processados[campo]}")

                    app.update_idletasks()

        progress_data["processed_lines"] = processed_lines
        logging.info(f"Número de blocos filtrados para {cidades[campo]}: {len(dados_filtrados)}")

        # Atualiza o status da cidade para "Concluído" ou "Nenhum dado encontrado"
        if dados_filtrados:
            cidade_status[campo] = "Concluído"
        else:
            cidade_status[campo] = "Nenhum dado encontrado"
        
        atualizar_status_cidades()
        return dados_filtrados

    except Exception as e:
        logging.error(f"Erro ao processar dados da cidade {cidades[campo]}: {e}")
        cidade_status[campo] = "Erro no processamento"
        atualizar_status_cidades()
        return []

def atualizar_status_cidades():
    # Limpa a Listbox e exibe o status atualizado das cidades
    cidade_listbox.delete(0, tk.END)
    for campo, status in cidade_status.items():
        cidade_listbox.insert(tk.END, f"{cidades[campo]}: {status}")

def executar_script():
    global processando
    start_time = time.time()
    try:
        if not input_file:
            messagebox.showerror("Erro", "Nenhum arquivo foi selecionado!")
            return

        total_lines = sum(1 for _ in open(input_file, 'r'))
        progress_label.config(text="Iniciando processamento...")
        progress_bar['value'] = 0

        # Inicializando o status das cidades
        for campo in cidades.keys():
            cidade_status[campo] = "Aguardando"
        atualizar_status_cidades()

        # Criando um Executor para processar as cidades simultaneamente
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for campo in cidades.keys():
                futures.append(executor.submit(filtrar_dados, input_file, campo, progress_label, progress_bar, app, start_time, total_lines))

            # Aguardar todas as tarefas terminarem e processar os dados
            for future in futures:
                dados_filtrados = future.result()
                if dados_filtrados:
                    output_file = os.path.join(diretorio_saida, f"{os.path.basename(input_file).replace('.txt', f'_{campo}.txt')}")
                    with open(output_file, 'w') as outfile:
                        outfile.writelines(dados_filtrados)
                    messagebox.showinfo("Sucesso", f"Arquivo {output_file} gerado com sucesso.")
                else:
                    messagebox.showinfo("Info", f"Nenhum dado encontrado para {campo}.")

    except Exception as e:
        messagebox.showerror("Erro", f"Erro no script: {e}")
    finally:
        processando = False
        progress_label.config(text="Processamento concluído.")
        btn_iniciar.config(state=tk.NORMAL)
        btn_pausar.config(state=tk.DISABLED)
        btn_retornar.config(state=tk.DISABLED)

# Função para selecionar o arquivo
def selecionar_arquivo():
    global input_file
    input_file = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
    if input_file:
        file_label.config(text=f"Arquivo selecionado: {input_file}")

# Função para selecionar o diretório de saída
def selecionar_diretorio_saida():
    global diretorio_saida
    diretorio_saida = filedialog.askdirectory()
    if diretorio_saida:
        directory_label.config(text=f"Diretório de saída selecionado: {diretorio_saida}")

# Interface Gráfica
app = tk.Tk()
app.title("Processador de Arquivos Simples Nacional")
app.geometry("600x600")
app.config(bg="#f0f0f0")

title_label = tk.Label(app, text="Processador de Arquivos", font=("Segoe UI", 18), bg='#f0f0f0', fg='#333')
title_label.pack(pady=20)

file_label = tk.Label(app, text="Nenhum arquivo selecionado", bg='#f0f0f0')
file_label.pack(pady=5)

btn_selecionar_arquivo = tk.Button(app, text="Selecionar Arquivo", command=selecionar_arquivo)
btn_selecionar_arquivo.pack(pady=5)

directory_label = tk.Label(app, text="Nenhum diretório selecionado", bg='#f0f0f0')
directory_label.pack(pady=5)

btn_selecionar_diretorio_saida = tk.Button(app, text="Selecionar Diretório de Saída", command=selecionar_diretorio_saida)
btn_selecionar_diretorio_saida.pack(pady=5)

btn_iniciar = tk.Button(app, text="Iniciar", state=tk.NORMAL, command=executar_script)
btn_iniciar.pack(pady=10)

btn_pausar = tk.Button(app, text="Pausar", state=tk.DISABLED, command=lambda: pausar())
btn_pausar.pack(pady=5)

btn_retornar = tk.Button(app, text="Retornar", state=tk.DISABLED, command=lambda: retornar())
btn_retornar.pack(pady=5)

# Barra de progresso
progress_label = tk.Label(app, text="Aguardando", bg='#f0f0f0')
progress_label.pack(pady=10)

progress_bar = ttk.Progressbar(app, length=400, mode="determinate", maximum=100)
progress_bar.pack(pady=10)

# Status da cidade
cidade_listbox = tk.Listbox(app, width=60, height=10, selectmode=tk.SINGLE)
cidade_listbox.pack(pady=20)

# Funções de controle (Pausar e Retornar)
def pausar():
    global pausado
    pausado = True
    btn_pausar.config(state=tk.DISABLED)
    btn_retornar.config(state=tk.NORMAL)

def retornar():
    global pausado
    pausado = False
    btn_retornar.config(state=tk.DISABLED)
    btn_pausar.config(state=tk.NORMAL)

# Atualizar engrenagem
gear_label = tk.Label(app, font=("Segoe UI", 20), bg='#f0f0f0', fg="#333")
gear_label.pack(pady=10)
atualizar_engrenagem()

app.mainloop()
