
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import time
from datetime import datetime, timedelta

# Lista de códigos de cidades
cidades = {
    '1032': 'CAMPOS DE JULIO',
    '9881': 'FIQUEIROPOLIS DOESTE',
    '8991': 'JAURU',
    '0137': 'LAMBARI DOESTE',
    '9177': 'MIRASSOL DOESTE',
    '9121': 'NS_LIVRAMENTO',
    '9875': 'PORTO ESPERIDIAO',
    '9879': 'RESERVA DO CABACAL',
    '8997': 'SALTO DO CEU',
    '8993': 'SAO JOSE DOS QUATRO MARCOS',
}

# Variáveis globais
processando = False
cancelar = False

def log_mensagem(log_path, mensagem):
    """Função para gravar mensagens no arquivo de log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, 'a') as log_file:
        log_file.write(f"[{timestamp}] {mensagem}\n")

def selecionar_arquivo():
    global input_file
    input_file = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
    if input_file:
        file_label.config(text=f"Arquivo selecionado: {os.path.basename(input_file)}")

def selecionar_diretorio_saida():
    global diretorio_saida
    diretorio_saida = filedialog.askdirectory()
    if diretorio_saida:
        output_label.config(text=f"Diretório de saída: {diretorio_saida}")

def calcular_estimativa(linhas_processadas, tempo_decorrido, total_linhas, suavizacao=0.9):
    if linhas_processadas == 0:
        return "Estimando..."
    
    tempo_por_linha = tempo_decorrido / linhas_processadas
    if hasattr(calcular_estimativa, "tempo_por_linha_prev"):
        tempo_por_linha = (suavizacao * calcular_estimativa.tempo_por_linha_prev +
                           (1 - suavizacao) * tempo_por_linha)
    calcular_estimativa.tempo_por_linha_prev = tempo_por_linha
    tempo_restante = tempo_por_linha * (total_linhas - linhas_processadas)
    return str(timedelta(seconds=int(tempo_restante)))

def processar_arquivo():
    global processando, cancelar
    processando = True
    cancelar = False

    if not input_file or not diretorio_saida:
        messagebox.showerror("Erro", "Selecione um arquivo e um diretório de saída!")
        return

    output_file = os.path.join(diretorio_saida, "resultado_filtrado.txt")
    log_file = os.path.join(diretorio_saida, "processamento_log.txt")
    total_linhas = sum(1 for _ in open(input_file, 'r'))
    linhas_processadas = 0
    bloco = []
    registros_filtrados = []
    start_time = time.time()
    codigo_encontrado = {codigo: 0 for codigo in cidades.keys()}  # Inicializa contagem de códigos encontrados

    log_mensagem(log_file, "Início do processamento.")
    log_mensagem(log_file, f"Arquivo de entrada: {input_file}")
    log_mensagem(log_file, f"Diretório de saída: {diretorio_saida}")
    log_mensagem(log_file, f"Total de linhas no arquivo: {total_linhas}")

    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            primeira_linha = infile.readline()
            if primeira_linha.startswith("AAAAA"):
                outfile.write(primeira_linha)  # Copia a primeira linha para o arquivo de saída

            for linha in infile:
                if cancelar:
                    progress_label.config(text="Processamento cancelado.")
                    log_mensagem(log_file, f"Processamento cancelado pelo usuário na linha {linhas_processadas}.")
                    processando = False
                    return

                linhas_processadas += 1
                progress = (linhas_processadas / total_linhas) * 100
                progress_bar['value'] = progress

                elapsed_time = time.time() - start_time
                estimativa = calcular_estimativa(linhas_processadas, elapsed_time, total_linhas)

                progress_label.config(
                    text=f"Processando: {progress:.2f}% concluído ({linhas_processadas}/{total_linhas} linhas)\n"
                         f"Tempo estimado: {estimativa}")
                app.update_idletasks()

                if linha.startswith("01000"):
                    bloco = [linha]
                elif linha.startswith("99999"):
                    bloco.append(linha)
                    if any(f'|{codigo}|' in registro for codigo in cidades.keys() for registro in bloco):
                        registros_filtrados.extend(bloco)
                        outfile.writelines(bloco)
                        for codigo in cidades.keys():
                            if any(f'|{codigo}|' in registro for registro in bloco):
                                codigo_encontrado[codigo] += 1  # Incrementa a contagem para o código encontrado
                                if codigo_encontrado[codigo] == 1 or codigo_encontrado[codigo] % 10 == 0:
                                    log_mensagem(log_file, f"Cidade {cidades[codigo]} ({codigo}) encontrada {codigo_encontrado[codigo]} vezes.")
                        log_mensagem(log_file, f"Bloco encontrado e salvo para códigos: {cidades.keys()}")
                else:
                    bloco.append(linha)

            total_linhas_processadas = len(registros_filtrados) + 2  # Inclui a primeira e a última linha

            outfile.write(f"ZZZZZ|{total_linhas_processadas:08d}\n")  # Adiciona a última linha com o total de linhas

        for codigo, contagem in codigo_encontrado.items():
            log_mensagem(log_file, f"Código {codigo} ({cidades[codigo]}) encontrado {contagem} vezes.")

        progress_label.config(text="Processamento concluído com sucesso!")
        log_mensagem(log_file, f"Processamento concluído. Arquivo gerado: {output_file}")
        messagebox.showinfo("Sucesso", f"Arquivo gerado: {output_file}")

    except Exception as e:
        error_message = f"Erro no processamento: {e}"
        log_mensagem(log_file, error_message)
        messagebox.showerror("Erro", error_message)
    finally:
        processando = False
        log_mensagem(log_file, "Finalizando processamento.")

def cancelar_processamento():
    global cancelar
    if processando:
        cancelar = True
        progress_label.config(text="Cancelando processamento...")
    else:
        messagebox.showinfo("Info", "Nenhum processamento em andamento.")

# Configuração da interface gráfica
app = tk.Tk()
app.title("Processamento de Arquivo")
app.geometry("500x400")

# Widgets
btn_iniciar = tk.Button(app, text="Iniciar Processamento", command=lambda: threading.Thread(target=processar_arquivo).start())
btn_iniciar.pack(pady=10)

btn_cancelar = tk.Button(app, text="Cancelar Processamento", command=cancelar_processamento)
btn_cancelar.pack(pady=5)

progress_bar = ttk.Progressbar(app, length=400, mode="determinate")
progress_bar.pack(pady=15)

progress_label = tk.Label(app, text="Aguardando início...", font=("Arial", 10))
progress_label.pack(pady=5)

file_label = tk.Label(app, text="Nenhum arquivo selecionado.", font=("Arial", 10))
file_label.pack(pady=5)

btn_selecionar_arquivo = tk.Button(app, text="Selecionar Arquivo", command=selecionar_arquivo)
btn_selecionar_arquivo.pack(pady=5)

output_label = tk.Label(app, text="Nenhum diretório selecionado.", font=("Arial", 10))
output_label.pack(pady=5)

btn_selecionar_saida = tk.Button(app, text="Selecionar Diretório de Saída", command=selecionar_diretorio_saida)
btn_selecionar_saida.pack(pady=5)

# Iniciar o aplicativo....
app.mainloop()
