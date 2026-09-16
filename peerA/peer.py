import socket # Para criar os sockets
import threading # Para o programa fazer várias coisas simultaneamente
import os # Para trabalhar com arquivos e diretórios
import time # Para fazer o programa esperar
import json # Para ler o config.json

# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Abre o arquivo config.json no modo leitura
with open("config.json", "r") as arquivo:
    # Converte o JSON para um dicionário python
    config = json.load(arquivo)

PEER_ID = config["peer_id"] # Pega o ID deste peer
HOST = config["host"] # IP que o peer vai ficar escutando
PORT = config["port"] # Porta UDP que pertence a este peer
PEERS = config["peers"] # Lista com os outros peers conhecidos

PASTA = "tmp" # Pasta que vai ser sincronizada
CHUNK_SIZE = 1024

arquivos_recebidos_rede = set() # Guarda os arquivos que foram recebidos pela rede

arquivos_removidos_rede = set() # Guarda os arquivos que foram removidos pela rede

# Cria a pasta tmp caso ela não exista
os.makedirs(PASTA, exist_ok=True)


# ============================================================
# SOCKET UDP
# ============================================================

# Cria um socket
sock = socket.socket(
    socket.AF_INET, # IPv4
    socket.SOCK_DGRAM # UDP
)

# Associa o socket ao IP e a porta que foram definidos antes. Agora o peer vai ficar escutando nessa porta
sock.bind((HOST, PORT))

print(f"Peer {PEER_ID} iniciado em {HOST}:{PORT}")


# ============================================================
# ENVIO DE MENSAGENS
# ============================================================

def enviar_mensagem(peer, mensagem):

    # Monta o endereço do peer que irá receber a mensagem
    endereco = (peer["host"], peer["port"])

    # Converte a mensagem de texto para bytes
    dados = mensagem.encode()

    # Envia a mensagem via UDP
    sock.sendto(dados, endereco)

    print(f"[ENVIO] {peer['id']} <- {mensagem}")


def enviar_para_todos(mensagem):
    
    # Percorre todos os peers e envia a mesma mensagem para cada um
    for peer in PEERS:
        enviar_mensagem(peer, mensagem)


# ============================================================
# MONITORAMENTO DO TMP
# ============================================================

def listar_arquivos():

    # Retorna os nomes dos arquivos presente na pasta tmp
    return set(os.listdir(PASTA))


def monitorar_pasta():

    # Salva quais arquivos existem no início do monitoramento
    arquivos_anteriores = listar_arquivos()

    # Fica monitorando infinitamente
    while True:

        time.sleep(2) # Espera 2 seg para verificar novamente

        arquivos_atuais = listar_arquivos()

        # Arquivos adicionados
        adicionados = arquivos_atuais - arquivos_anteriores

        # Arquivos removidos
        removidos = arquivos_anteriores - arquivos_atuais

        # Alerta arquivos novos
        for arquivo in adicionados:

            # Verifica se o arquivo foi recebido pela rede
            if arquivo in arquivos_recebidos_rede:

                # Remove da lista porque já tratamos esse arquivo
                arquivos_recebidos_rede.remove(arquivo)

                continue

            print(f"[NOVO ARQUIVO] {arquivo}")

            mensagem = f"ANNOUNCE|{arquivo}"

            enviar_para_todos(mensagem)

        # Alerta arquivos removidos
        for arquivo in removidos:

            # Verifica se o arquivo foi removido pela rede
            if arquivo in arquivos_removidos_rede:

                # Remove da lista porque já tratamos essa remoção
                arquivos_removidos_rede.remove(arquivo)

                continue

            print(f"[ARQUIVO REMOVIDO] {arquivo}")

            mensagem = f"REMOVE|{arquivo}"

            enviar_para_todos(mensagem)

        # Atualiza para a próxima verificação com os arquivos atuais
        arquivos_anteriores = arquivos_atuais


# ============================================================
# SERVIDOR UDP
# ============================================================

def servidor():

    while True:

        try:

            dados, endereco = sock.recvfrom(65535)

        except ConnectionResetError:

            print("[ERRO] Peer remoto não está disponível.")

            continue

        if dados.startswith(b"DATA|"):

            receber_dados(
                dados,
                endereco
            )

        else:

            mensagem = dados.decode()

            print(
                f"[RECEBIDO] {endereco}: {mensagem}"
            )

            processar_mensagem(
                mensagem,
                endereco
            )


# ============================================================
# PROCESSAMENTO DAS MENSAGENS
# ============================================================

def processar_mensagem(mensagem, endereco):

    # Divide a mensagem
    partes = mensagem.split("|")

    tipo = partes[0]


    # --------------------------------------------------------
    # ANNOUNCE
    # --------------------------------------------------------

    if tipo == "ANNOUNCE":

        nome_arquivo = partes[1]

        print(
            f"Peer anunciou o arquivo: {nome_arquivo}"
        )

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        # Se ainda não temos o arquivo,
        # pedimos para quem anunciou
        if not os.path.exists(caminho):

            mensagem = f"GET|{nome_arquivo}"

            sock.sendto(
                mensagem.encode(),
                endereco
            )


    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    elif tipo == "GET":

        nome_arquivo = partes[1]

        enviar_arquivo(
            nome_arquivo,
            endereco
        )


    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    elif tipo == "DATA":

        receber_dados(
            partes,
            endereco
        )


    # --------------------------------------------------------
    # REMOVE
    # --------------------------------------------------------

    elif tipo == "REMOVE":

        nome_arquivo = partes[1]

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        if os.path.exists(caminho):

            arquivos_removidos_rede.add(nome_arquivo)

            os.remove(caminho)

            print(
                f"[REMOVIDO] {nome_arquivo}"
            )

    # --------------------------------------------------------
    # LIST
    # --------------------------------------------------------

    elif tipo == "LIST":

        arquivos = listar_arquivos()

        mensagem = "FILES|" + ",".join(arquivos)

        sock.sendto(
            mensagem.encode(),
            endereco
        )


    # --------------------------------------------------------
    # FILES
    # --------------------------------------------------------

    elif tipo == "FILES":

        arquivos = partes[1].split(",")

        for arquivo in arquivos:

            if arquivo:

                caminho = os.path.join(
                    PASTA,
                    arquivo
                )

                if not os.path.exists(caminho):

                    mensagem = f"GET|{arquivo}"

                    sock.sendto(
                        mensagem.encode(),
                        endereco
                    )


# ============================================================
# ENVIO DO ARQUIVO
# ============================================================

def enviar_arquivo(nome_arquivo, endereco):

    caminho = os.path.join(
        PASTA,
        nome_arquivo
    )

    # Verifica se o arquivo existe
    if not os.path.exists(caminho):
        return

    print(
        f"[ENVIANDO] {nome_arquivo}"
    )

    # Abre o arquivo no modo binário
    with open(caminho, "rb") as arquivo:

        # Lê todo o conteúdo do arquivo
        dados_arquivo = arquivo.read()

    # Calcula quantos pedaços serão necessários
    total_pedacos = (
        len(dados_arquivo) + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    # Percorre o arquivo pedaço por pedaço
    for numero_pedaco in range(total_pedacos):

        inicio = numero_pedaco * CHUNK_SIZE

        fim = inicio + CHUNK_SIZE

        pedaco = dados_arquivo[inicio:fim]

        # Monta o cabeçalho da mensagem
        cabecalho = (
            f"DATA|{nome_arquivo}|"
            f"{numero_pedaco}|{total_pedacos}|"
        ).encode()

        # Junta o cabeçalho com os bytes do arquivo
        mensagem = cabecalho + pedaco

        # Envia o pedaço pelo UDP
        sock.sendto(
            mensagem,
            endereco
        )

        print(
            f"[ENVIO] pedaço "
            f"{numero_pedaco + 1}/{total_pedacos}"
        )


# ============================================================
# RECEBIMENTO DO ARQUIVO
# ============================================================

# Guarda temporariamente os pedaços dos arquivos recebidos
arquivos_recebidos = {}


def receber_dados(dados, endereco):

    # Divide a mensagem somente até o quarto "|"
    partes = dados.split(b"|", 4)

    # Verifica se a mensagem possui todas as partes
    if len(partes) != 5:
        return

    # Converte as partes do cabeçalho para texto
    tipo = partes[0].decode()
    nome_arquivo = partes[1].decode()
    numero_pedaco = int(partes[2].decode())
    total_pedacos = int(partes[3].decode())

    # O conteúdo do arquivo continua sendo bytes
    pedaco = partes[4]

    print(
        f"[RECEBIDO] {nome_arquivo} "
        f"pedaço {numero_pedaco + 1}/{total_pedacos}"
    )

    # Cria a estrutura para esse arquivo
    if nome_arquivo not in arquivos_recebidos:

        arquivos_recebidos[nome_arquivo] = {
            "total": total_pedacos,
            "pedacos": {}
        }

    # Salva o pedaço recebido
    arquivos_recebidos[nome_arquivo]["pedacos"][
        numero_pedaco
    ] = pedaco

    # Verifica quantos pedaços já foram recebidos
    quantidade_recebida = len(
        arquivos_recebidos[nome_arquivo]["pedacos"]
    )

    # Se recebeu todos os pedaços
    if quantidade_recebida == total_pedacos:

        # Marca que o arquivo veio da rede
        arquivos_recebidos_rede.add(nome_arquivo)

        caminho = os.path.join(
            PASTA,
            nome_arquivo
        )

        # Cria o arquivo final
        with open(caminho, "wb") as arquivo:

            for numero in range(total_pedacos):

                arquivo.write(
                    arquivos_recebidos[nome_arquivo]["pedacos"][numero]
                )

        print(
            f"[ARQUIVO COMPLETO] {nome_arquivo}"
        )

        # Remove os pedaços da memória
        del arquivos_recebidos[nome_arquivo]


# ============================================================
# SINCRONIZAÇÃO INICIAL
# ============================================================

def sincronizar():

    print("[SYNC] Solicitando arquivos aos peers...")

    mensagem = "LIST"

    enviar_para_todos(mensagem)


# ============================================================
# INICIALIZAÇÃO
# ============================================================

thread_servidor = threading.Thread(
    target=servidor,
    daemon=True
)

thread_servidor.start()


thread_monitor = threading.Thread(
    target=monitorar_pasta,
    daemon=True
)

thread_monitor.start()


# Sincronização quando o peer inicia
sincronizar()


print("Peer executando...")


while True:

    time.sleep(1)