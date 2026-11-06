"""
Servidor de leilão online em tempo real usando Sockets UDP.

Cada datagrama carrega uma mensagem de texto (UTF-8).

Cliente -> Servidor (seq = número de sequência da requisição):
    <seq> JOIN          entra no leilão
    <seq> BID <valor>   envia um lance
    <seq> STATUS        consulta o estado atual do leilão
    <seq> QUIT          sai do leilão
    PING                mantém o cliente registrado (sem resposta)

Servidor -> Cliente:
    RESP <seq> WELCOME <id> <item>
    RESP <seq> OK <valor>
    RESP <seq> ERRO <motivo>
    RESP <seq> STATUS <item> <valor> <id> <seg>
    RESP <seq> BYE
    NEW_BID <valor> <id>        notificação de novo lance máximo
    END <valor> <id>            leilão encerrado
"""

import argparse
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation

TAM_MAX = 1024
EXPIRACAO = 30  # segundos sem receber nada do cliente até removê-lo


class Cliente:
    def __init__(self, addr, cid):
        self.addr = addr
        self.id = cid
        self.visto = time.monotonic()
        # Último seq respondido e a resposta dada. Se o cliente retransmitir a
        # mesma requisição (porque a resposta se perdeu), reenviamos a resposta
        # em vez de processar o comando de novo.
        self.ultimo_seq = 0
        self.ultima_resp = None


class Leilao:
    def __init__(self, sock, item, lance_minimo, duracao):
        self.sock = sock
        self.item = item
        self.lance_atual = lance_minimo
        self.proponente = None
        self.fim = time.monotonic() + duracao
        self.encerrado = False
        self.clientes = {}  # endereço (ip, porta) -> Cliente
        self.contador = 0
        # Protege o estado do leilão e a tabela de clientes. Os datagramas são
        # tratados por várias threads, então a verificação e o registro do lance
        # precisam acontecer dentro do mesmo lock para serem atômicos.
        self.lock = threading.Lock()

    def enviar(self, addr, msg):
        try:
            self.sock.sendto(msg.encode("utf-8"), addr)
        except OSError:
            pass

    def restante(self):
        return max(0, int(self.fim - time.monotonic()))

    def tratar(self, dados, addr):
        try:
            partes = dados.decode("utf-8").split()
        except UnicodeDecodeError:
            return
        if not partes:
            return

        with self.lock:
            cliente = self.clientes.get(addr)
            if cliente:
                cliente.visto = time.monotonic()

            if partes[0].upper() == "PING":
                return

            if not partes[0].isdigit() or len(partes) < 2:
                self.enviar(addr, "RESP 0 ERRO formato: <seq> <comando> [args]")
                return
            seq = int(partes[0])
            cmd = partes[1].upper()
            args = partes[2:]

            if cliente and seq <= cliente.ultimo_seq:
                if seq == cliente.ultimo_seq and cliente.ultima_resp:
                    self.enviar(addr, cliente.ultima_resp)
                return

            if cmd == "JOIN":
                if self.encerrado:
                    resp = "ERRO leilao encerrado"
                else:
                    if not cliente:
                        self.contador += 1
                        cliente = Cliente(addr, f"licitante-{self.contador}")
                        self.clientes[addr] = cliente
                        print(f"[ENTROU] {cliente.id} {addr}")
                    resp = f"WELCOME {cliente.id} {self.item}"
            elif not cliente:
                self.enviar(addr, f"RESP {seq} ERRO envie JOIN primeiro")
                return
            elif cmd == "BID":
                valor = parse_valor(args[0]) if len(args) == 1 else None
                if valor is None:
                    resp = "ERRO uso: BID <valor positivo>"
                else:
                    resp = self.dar_lance(cliente, valor)
            elif cmd == "STATUS":
                resp = (f"STATUS {self.item} {self.lance_atual:.2f} "
                        f"{self.proponente or '-'} {self.restante()}")
            elif cmd == "QUIT":
                del self.clientes[addr]
                print(f"[SAIU] {cliente.id}")
                resp = "BYE"
            else:
                resp = "ERRO comando desconhecido (use BID <valor>, STATUS ou QUIT)"

            resp = f"RESP {seq} {resp}"
            cliente.ultimo_seq = seq
            cliente.ultima_resp = resp
            self.enviar(addr, resp)

    def dar_lance(self, cliente, valor):
        """Chamado com self.lock já adquirido."""
        if self.encerrado or time.monotonic() >= self.fim:
            return "ERRO leilao encerrado"
        if valor <= self.lance_atual:
            return f"ERRO lance deve ser maior que {self.lance_atual:.2f}"

        self.lance_atual = valor
        self.proponente = cliente.id
        print(f"[LANCE] {cliente.id} -> {valor:.2f}")

        # Notifica os outros ainda dentro do lock, para que todos recebam os
        # NEW_BID na mesma ordem em que os lances foram registrados.
        aviso = f"NEW_BID {valor:.2f} {cliente.id}"
        for outro in self.clientes.values():
            if outro is not cliente:
                self.enviar(outro.addr, aviso)
        return f"OK {valor:.2f}"

    def remover_inativos(self):
        with self.lock:
            limite = time.monotonic() - EXPIRACAO
            for addr, c in list(self.clientes.items()):
                if c.visto < limite:
                    del self.clientes[addr]
                    print(f"[EXPIROU] {c.id}")

    def encerrar(self):
        with self.lock:
            self.encerrado = True
            msg = f"END {self.lance_atual:.2f} {self.proponente or '-'}"
            enderecos = [c.addr for c in self.clientes.values()]
        print(f"[FIM] {msg}")
        # UDP não garante entrega, então o aviso de fim é enviado 3 vezes.
        for _ in range(3):
            for addr in enderecos:
                self.enviar(addr, msg)
            time.sleep(0.2)


def parse_valor(texto):
    try:
        valor = Decimal(texto.replace(",", "."))
    except InvalidOperation:
        return None
    if not valor.is_finite() or valor <= 0:
        return None
    return valor.quantize(Decimal("0.01"))


def main():
    p = argparse.ArgumentParser(description="Servidor de leilão UDP")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--porta", type=int, default=5000)
    p.add_argument("--item", default="Relogio-Antigo")
    p.add_argument("--minimo", default="100", help="lance inicial (lances precisam superá-lo)")
    p.add_argument("--duracao", type=int, default=120, help="duração do leilão em segundos")
    args = p.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.porta))
    sock.settimeout(1)  # permite interromper com Ctrl+C no Windows

    leilao = Leilao(sock, args.item.replace(" ", "-"),
                    parse_valor(args.minimo) or Decimal("0.00"), args.duracao)

    def cronometro():
        while time.monotonic() < leilao.fim:
            time.sleep(1)
            leilao.remover_inativos()
        leilao.encerrar()
        sock.close()

    threading.Thread(target=cronometro, daemon=True).start()
    print(f"Leilão de '{leilao.item}' aberto em {args.host}:{args.porta}/UDP "
          f"(mínimo {leilao.lance_atual:.2f}, {args.duracao}s)")

    with ThreadPoolExecutor(max_workers=16) as trabalhadores:
        try:
            while True:
                try:
                    dados, addr = sock.recvfrom(TAM_MAX)
                except (socket.timeout, ConnectionResetError):
                    # No Windows, um datagrama enviado a um cliente que já
                    # fechou faz o próximo recvfrom() lançar ConnectionResetError.
                    continue
                except OSError:
                    break
                trabalhadores.submit(leilao.tratar, dados, addr)
        except KeyboardInterrupt:
            leilao.encerrar()


if __name__ == "__main__":
    main()
