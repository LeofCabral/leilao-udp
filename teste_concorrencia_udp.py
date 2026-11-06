"""
Teste automático da versão UDP: sobe o servidor, registra vários licitantes que
disparam lances ao mesmo tempo e verifica que
  1. o lance final é o maior lance aceito (registro atômico);
  2. os lances aceitos formam uma sequência estritamente crescente;
  3. um observador recebeu um NEW_BID para cada lance aceito, na mesma ordem;
  4. uma requisição retransmitida recebe a mesma resposta sem ser processada de novo.
"""

import os
import random
import socket
import subprocess
import sys
import threading
import time

PORTA = 5056
SERVIDOR = ("127.0.0.1", PORTA)
N_CLIENTES = 20
LANCES_POR_CLIENTE = 25


class Licitante:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(2)
        self.seq = 0

    def requisitar(self, comando, seq=None):
        if seq is None:
            self.seq += 1
            seq = self.seq
        self.sock.sendto(f"{seq} {comando}".encode(), SERVIDOR)
        while True:  # ignora NEW_BID de outros até chegar a resposta
            partes = self.sock.recv(1024).decode().split()
            if partes[0] == "RESP" and partes[1] == str(seq):
                return partes[2:]


def esperar_servidor():
    sonda = Licitante()
    sonda.sock.settimeout(0.2)
    for _ in range(50):
        try:
            sonda.requisitar("JOIN")
            return
        except (socket.timeout, ConnectionResetError):
            time.sleep(0.1)
    raise RuntimeError("servidor não subiu")


def main():
    pasta = os.path.dirname(os.path.abspath(__file__))
    servidor = subprocess.Popen(
        [sys.executable, os.path.join(pasta, "servidor_udp.py"),
         "--porta", str(PORTA), "--duracao", "15", "--minimo", "100"],
        stdout=subprocess.DEVNULL)
    try:
        esperar_servidor()

        obs = Licitante()
        obs.requisitar("JOIN")
        obs.sock.settimeout(None)
        notificacoes = []

        def observar():
            while True:
                try:
                    msg = obs.sock.recv(1024).decode()
                except OSError:
                    return
                if msg.startswith("NEW_BID"):
                    notificacoes.append(msg.split()[1])

        threading.Thread(target=observar, daemon=True).start()

        aceitos = []
        aceitos_lock = threading.Lock()
        licitantes = [Licitante() for _ in range(N_CLIENTES)]
        for lic in licitantes:
            lic.requisitar("JOIN")
        largada = threading.Barrier(N_CLIENTES)

        def disputar(lic):
            largada.wait()  # todos começam juntos para maximizar a concorrência
            for _ in range(LANCES_POR_CLIENTE):
                resp = lic.requisitar(f"BID {random.randint(101, 5000)}")
                if resp[0] == "OK":
                    with aceitos_lock:
                        aceitos.append(resp[1])

        threads = [threading.Thread(target=disputar, args=(lic,)) for lic in licitantes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(0.5)

        consulta = Licitante()
        consulta.requisitar("JOIN")
        status = consulta.requisitar("STATUS")

        # Retransmissão: mesmo seq de um lance acima do atual deve devolver a
        # resposta original e não registrar um segundo lance.
        valor = float(status[2]) + 1
        primeira = consulta.requisitar(f"BID {valor:.2f}")
        repetida = consulta.requisitar(f"BID {valor:.2f}", seq=consulta.seq)
        aceitos.append(primeira[1])
        time.sleep(0.3)

        maior = max(aceitos, key=float)
        valores = [float(v) for v in notificacoes]
        print(f"lances enviados: {N_CLIENTES * LANCES_POR_CLIENTE}, aceitos: {len(aceitos) - 1}")
        print(f"STATUS final: {' '.join(status)}")
        assert status[2] == max(aceitos[:-1], key=float), "lance final diferente do maior aceito"
        assert primeira == repetida == ["OK", f"{valor:.2f}"], "retransmissão processada de novo"
        assert valores == sorted(valores) and len(set(valores)) == len(valores), "sequência não crescente"
        assert sorted(notificacoes, key=float) == sorted(aceitos, key=float), "observador perdeu notificações"
        print(f"maior lance após retransmissão: {maior}")
        print("OK: registro atômico, notificações consistentes e retransmissão sem duplicar.")
    finally:
        servidor.terminate()


if __name__ == "__main__":
    main()
