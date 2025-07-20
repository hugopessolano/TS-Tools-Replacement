# TS-Tools Replacement

Este repositório contém o código-fonte do **TS-Tools Replacement**, uma biblioteca Python projetada para simplificar e otimizar a interação com a **API da Tiendanube**.  
Seu principal objetivo é facilitar a realização de requisições (individuais ou em massa), gerenciando de forma eficiente os limites de taxa impostos para cada loja.

## Vídeo Demonstrativo (bem resumido)
https://drive.google.com/file/d/1TGpAv7oRqrUDhZMkNel1sRHxn-I1NuQ8/view?usp=sharing

## Instalação

Embora o projeto ainda esteja em desenvolvimento e não esteja completo, seus componentes já estão funcionais e podem ser utilizados.

Para instalá-lo, siga os passos abaixo:

1. **Clone o repositório:**
   ```bash
   git clone git@github.com:hugopessolano/TS-Tools-Replacement.git
   cd TS-Tools-replacement-repo
   ```

2. **Crie e ative um ambiente virtual:**
   É uma boa prática utilizar ambientes virtuais para isolar as dependências do projeto.

   * No Linux/macOS:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
   * No Windows:
     ```bash
     python -m venv venv
     .\venv\Scripts\activate
     ```
   *(Você verá `(venv)` no início da linha de comando se a ativação for bem-sucedida)*

3. **Instale as dependências:**
   O arquivo `requirements.txt` contém todas as bibliotecas necessárias.
   ```bash
   pip install -r requirements.txt
   ```

Pronto! Seu ambiente está pronto para começar a usar ou desenvolver a biblioteca.

## Documentação Técnica

A documentação técnica detalhada do projeto — incluindo descrições dos módulos, classes, funções e os esquemas Pydantic utilizados — é gerada em formato HTML.

Você pode acessá-la abrindo o seguinte arquivo em seu navegador:

**docs/build/html/index.html**

Essa documentação é a principal referência para entender a estrutura interna e o uso dos diferentes componentes da biblioteca.

GitHub Pages: https://hugopessolano.github.io/TS-Tools-Replacement/

![image](https://github.com/user-attachments/assets/76b303b8-b0df-47d4-8c45-c1eab45b3412)

## Sobre o Projeto

**TS-Tools Replacement** foi criado como uma alternativa moderna e aprimorada a ferramentas anteriores, com foco específico na interação com a API da Tiendanube. É voltado principalmente para desenvolvedores que precisam criar scripts para extrair ou manipular dados da Tiendanube de forma programática.

### Principais Funcionalidades

* **Interface Simplificada para a API da Tiendanube:** Abstrai complexidades para facilitar chamadas à API.
* **Gerenciamento Avançado de Rate Limits:** Implementa uma estratégia para maximizar o uso da cota da API, combinando um *burst* inicial com um ritmo constante, utilizando a biblioteca `httpx` e semáforos. Detecta ou permite configurar limites específicos por loja.
* **Processamento de Dados com Pandas:** A ferramenta utiliza a biblioteca `Pandas` junto com tipos de dados personalizados para lidar eficientemente com grandes volumes de requisições e respostas. Atualmente não persiste os dataframes, mas a arquitetura permite escalar nesse sentido.
* **Validação Robusta com Pydantic:** Utiliza extensivamente esquemas do Pydantic para:
  * Validar a configuração da biblioteca.
  * Definir e validar a estrutura das requisições e endpoints.
  * Validar dados (respostas ou entradas).
  * Configurar os parâmetros de rate limiting.
* **Logging Persistente em Banco de Dados:** Todas as operações de requisição — incluindo parâmetros, metadados, sucesso/falha e dados de resposta — são registradas em um banco de dados para auditoria, depuração e análise futura.
* **Arquitetura Modular:** O código está organizado em módulos com responsabilidades claras (`request_manager`, `dataframe_manager`, `schemas`, `log_db`, etc.), facilitando sua manutenção e extensão.

### Estado Atual

O projeto está em **desenvolvimento ativo**. A funcionalidade principal (conexão, gerenciamento de rate limit, logging) já está implementada, mas ainda falta desenvolver uma camada de interação com o usuário (como uma CLI ou interface gráfica). Atualmente, é utilizado como um framework importado em scripts Python. Tratamento avançado de erros e lógica de repetição estão planejados para versões futuras.
