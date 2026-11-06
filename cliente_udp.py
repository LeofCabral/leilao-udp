"""
Cliente do leilão online usando Sockets UDP.

Como o UDP não garante entrega, cada comando leva um número de sequência e é
reenviado se a resposta não chegar a tempo. Uma thread fica recebendo os
datagramas o tempo todo, então as notificações NEW_BID aparecem na hora.
"""

import argparse
import os
import socket
import threading
import time

TAM_MAX = 1024
TIMEOUT = 1.0
TENTATIVAS = 5
INTERVALO_PING = 10


def formatar(msg):
    partes = msg.split()
    tipo = partes[0] if partes else ""
    if tipo == "WELCOME":
        return f"Conectado como {partes[1]} no leilão de '{partes[2]}'."
    if tipo == "OK":
        return f"Seu lance de {partes[1]} foi registrado. Você está vencendo!"
    if tipo == "NEW_BID":
        return f"*** NOVO LANCE MÁXIMO: {partes[1]} por {partes[2]} ***"
    if tipo == "STATUS":
        _, item, valor, quem, seg = partes
        quem = "nenhum lance ainda" if quem == "-" else quem
        return f"Item: {item} | Lance atual: {valor} ({quem}) | Restam {seg}s"
    if tipo == "END":
        quem = "sem vencedor" if partes[2] == "-" else f"vencedor {partes[2]}"
        return f"=== LEILÃO ENCERRADO: {quem}, valor {partes[1]} ==="
    if tipo == "ERRO":
        return "Erro: " + msg[5:]
    if tipo == "BYE":
        return "Você saiu do leilão."
    return msg


def mostrar(msg):
    try:
        texto = formatar(msg)
    except (IndexError, ValueError):
        texto = msg
    print("\n" + texto, flush=True)
    print("> ", end="", flush=True)


class Conexao:
    def __init__(self, host, porta):
        self.servidor = (socket.gethostbyname(host), porta)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0))  # porta local escolhida pelo sistema
        self.seq = 0
        self.pendente = None  # (seq, threading.Event, [resposta])
        self.lock = threading.Lock()

    def receber(self):
        while True:
            try:
                dados, addr = self.sock.recvfrom(TAM_MAX)
            except ConnectionResetError:
                continue
            except OSError:
                return
            if addr != self.servidor:
                continue  # ignora datagramas de outras origens
            msg = dados.decode("utf-8", errors="replace").strip()
            partes = msg.split(maxsplit=2)

            if partes and partes[0] == "RESP" and len(partes) == 3:
                with self.lock:
                    if self.pendente and str(self.pendente[0]) == partes[1]:
                        self.pendente[2].append(partes[2])
                        self.pendente[1].set()
                # respostas atrasadas de comandos antigos são ignoradas
            elif partes and partes[0] == "END":
                mostrar(msg)
                os._exit(0)
            else:
                mostrar(msg)

    def requisitar(self, comando):
        """Envia o comando e espera a resposta, reenviando se ela não chegar."""
        self.seq += 1
        evento = threading.Event()
        resposta = []
        with self.lock:
            self.pendente = (self.seq, evento, resposta)
        dados = f"{self.seq} {comando}".encode("utf-8")
        for _ in range(TENTATIVAS):
            self.sock.sendto(dados, self.servidor)
            if evento.wait(TIMEOUT):
                return resposta[0]
        return None

    def ping(self):
        while True:
            time.sleep(INTERVALO_PING)
            try:
                self.sock.sendto(b"PING", self.servidor)
            except OSError:
                return


def main():
    p = argparse.ArgumentParser(description="Cliente de leilão UDP")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--porta", type=int, default=5000)
    args = p.parse_args()

    con = Conexao(args.host, args.porta)
    threading.Thread(target=con.receber, daemon=True).start()

    resp = con.requisitar("JOIN")
    if resp is None:
        print("Servidor não respondeu.")
        return
    print(formatar(resp))
    if not resp.startswith("WELCOME"):
        return
    threading.Thread(target=con.ping, daemon=True).start()

    print("Comandos: BID <valor> | STATUS | QUIT")
    try:
        while True:
            cmd = input("> ").strip()
            if not cmd:
                continue
            resp = con.requisitar(cmd)
            if resp is None:
                print("Sem resposta do servidor (tente novamente).")
                continue
            try:
                print(formatar(resp))
            except (IndexError, ValueError):
                print(resp)
            if cmd.upper() == "QUIT":
                break
    except (EOFError, KeyboardInterrupt):
        pass
    con.sock.close()


if __name__ == "__main__":
    main()
