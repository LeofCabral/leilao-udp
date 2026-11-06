<h1 align="center">Leilão Online em Tempo Real com Sockets UDP</h1>

<p align="center">Leonardo Ferreira Cabral(sem grupo)</p>

## 📖 Sobre <a name = "sobre"></a>

Versão com Sockets UDP do sistema distribuído cliente-servidor que gerencia um leilão online em tempo real para um item específico. O servidor mantém o registro de todos os licitantes e controla o estado do leilão: o lance atual (valor e ID do proponente) e o tempo restante. Cada cliente pode enviar lances com o comando `BID <valor>` e consultar o estado com `STATUS`.

O servidor recebe os datagramas em um único socket e entrega cada um a um conjunto de threads (`ThreadPoolExecutor`). Por isso, lances de clientes diferentes são processados ao mesmo tempo. A validação e o registro de um lance acontecem dentro de um mesmo `threading.Lock`, então lances concorrentes são registrados de forma atômica. Sempre que um novo lance válido é registrado, o servidor envia imediatamente um datagrama `NEW_BID` para todos os outros licitantes registrados, ainda dentro da seção crítica, para que as notificações saiam na mesma ordem em que os lances foram registrados.


## 🏁 Primeiros Passos <a name = "primeiros_passos"></a>

As instruções abaixo permitem executar o projeto na sua máquina local.

### Pré-requisitos

O projeto usa apenas a biblioteca padrão do Python, então basta ter o Python 3.8 ou superior instalado.

```bash
# Verifique a versão do Python instalada
python --version
```

### Instalação

Não há dependências externas. Basta clonar o repositório:

```bash
# Clone o repositório e entre na pasta
git clone https://github.com/LeofCabral/leilao-udp.git
cd leilao-udp
```

## 📱 Uso <a name = "uso"></a>

Inicie primeiro o servidor e, em seguida, um ou mais clientes, cada um em um terminal diferente.

### Executando o Servidor

```bash
# Abre o leilão na porta 5000/UDP, com lance mínimo de 100 e duração de 120 segundos
python servidor_udp.py --porta 5000 --item Relogio-Antigo --minimo 100 --duracao 120
```

### Executando o Cliente

```bash
# Registra um licitante no servidor (abra um terminal para cada licitante)
python cliente_udp.py --host 127.0.0.1 --porta 5000
```

Comandos disponíveis no cliente:

```bash
BID 150    # envia um lance de 150
STATUS     # mostra o lance atual, o proponente e o tempo restante
QUIT       # sai do leilão
```

### Protocolo

Cada datagrama carrega uma mensagem de texto. Mensagens do cliente:

```bash
<seq> JOIN           # entra no leilão
<seq> BID <valor>    # envia um lance
<seq> STATUS         # consulta o estado
<seq> QUIT           # sai do leilão
PING                 # mantém o cliente registrado (sem resposta)
```

Mensagens do servidor:

```bash
RESP <seq> WELCOME <id> <item>                # boas-vindas, com o ID do licitante
RESP <seq> OK <valor>                         # lance aceito
RESP <seq> ERRO <motivo>                      # lance ou comando inválido
RESP <seq> STATUS <item> <valor> <id> <seg>   # estado atual do leilão
RESP <seq> BYE                                # saída confirmada
NEW_BID <valor> <id>                          # notificação de novo lance máximo (enviada aos outros clientes)
END <valor> <id>                              # fim do leilão e vencedor
```

### Teste de Concorrência

```bash
# Sobe um servidor próprio e simula 20 licitantes enviando 25 lances cada, ao mesmo tempo
python teste_concorrencia_udp.py
```

O teste verifica se o lance final é o maior lance aceito, se os lances aceitos formam uma sequência crescente, se um cliente observador recebeu um `NEW_BID` para cada lance aceito e se um comando retransmitido recebe a mesma resposta sem registrar o lance duas vezes. Ao final, deve exibir `OK: registro atômico, notificações consistentes e retransmissão sem duplicar.`

## ⛏️ Tecnologias Utilizadas <a name = "tecnologias"></a>

- [Python3](https://www.python.org/) - Linguagem de programação
- [socket](https://docs.python.org/3/library/socket.html) - Comunicação UDP entre cliente e servidor
- [threading](https://docs.python.org/3/library/threading.html) - Sincronização com `Lock` e threads de recebimento no cliente
- [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) - Conjunto de threads que processa os datagramas no servidor

---
