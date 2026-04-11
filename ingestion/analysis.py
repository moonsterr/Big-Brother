import asyncio
from ollama import AsyncClient
from ingestion.base_prompt import build_prompt

async def run_analysis(post_content):
    system_prompt =  build_prompt()
    try:
        response = await AsyncClient().chat(
            model='llama3.1:8b',
            format='json',
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': post_content}
            ],
            options={
                "num_ctx": 16384,
                "temperature": 0.1,  
                "num_gpu": 1    ,  
                "format": "json"  
            }
        )
        return response['message']['content']
    
    except Exception as e:
        return f"Analysis Error: {str(e)}"