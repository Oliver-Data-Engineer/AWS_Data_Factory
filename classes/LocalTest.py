import os

def salvar_arquivo(caminho_pasta, extensao, conteudo,nome_arquivo):
    # Garante que a extensão comece com ponto
    if not extensao.startswith('.'):
        extensao = f'.{extensao}'
    
    # Define o nome do arquivo (exemplo genérico: arquivo_gerado)
    nome_arquivo = f"{nome_arquivo}{extensao}"
    caminho_completo = os.path.join(caminho_pasta, nome_arquivo)

    try:
        # Cria a pasta e subpastas se não existirem
        os.makedirs(caminho_pasta, exist_ok=True)

        # Salva o conteúdo no arquivo
        with open(caminho_completo, 'w', encoding='utf-8') as arquivo:
            arquivo.write(conteudo)
        
        print(f"Sucesso! Arquivo salvo em: {caminho_completo}")
    
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
