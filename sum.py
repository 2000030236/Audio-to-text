import requests
import logging

# Llama API Configuration
MAIN_LLAMA_URL = "http://97.99.18.159:11434/api/generate"
BACKUP_LLAMA_URL = "http://97.99.18.159:11435/api/generate"
# Model verified from tags: llama3.1:8b-instruct-q4_K_M
LLAMA_MODEL = "llama3.1:8b-instruct-q4_K_M"

import time

def call_llama_api(payload, timeout):
    """
    Calls Llama API with fallback logic:
    1. Try Main.
    2. If fails, wait 60s and retry Main.
    3. If still fails, use Backup.
    4. Next request will always check Main again.
    """
    # 1. Try Main
    try:
        logging.info(f"LLAMA: Attempting Main {MAIN_LLAMA_URL}")
        response = requests.post(MAIN_LLAMA_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.warning(f"LLAMA: Main failed: {e}. Waiting 60s to retry Main...")
        
        # 2. Wait for a minute and re-try
        time.sleep(60)
        try:
            logging.info(f"LLAMA: Retrying Main {MAIN_LLAMA_URL}")
            response = requests.post(MAIN_LLAMA_URL, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e2:
            # 3. If then also unable to connect, use backup
            logging.warning(f"LLAMA: Main retry failed: {e2}. Switching to Backup {BACKUP_LLAMA_URL}")
            try:
                response = requests.post(BACKUP_LLAMA_URL, json=payload, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except Exception as e3:
                logging.error(f"LLAMA: Backup also failed: {e3}")
                raise e3


def summarize_text(text):
    """
    Summarizes the given text using the Llama API.
    """
    if not text:
        return ""
    
    prompt = (
        "You are given the full raw transcript of a video. Your task is to:\n"
        "1. Read the transcript and identify the core message.\n"
        "2. Re-write the content into extremely concise, easy-to-understand key points.\n"
        "3. Use simple, everyday terminology (no jargon).\n\n"
        "Rules:\n"
        "- Do NOT copy sentences verbatim from the transcript.\n"
        "- Each point must be short (max 15-20 words).\n"
        "- Provide 5-8 bullet points total.\n\n"
        "Output Format:\n"
        "Title: [Concise Video Title]\n"
        "\n"
        "Key Points:\n"
        "- [Short point 1]\n"
        "- [Short point 2]\n"
        "- [Short point 3]\n"
        "- [Short point 4]\n"
        "- [Short point 5]\n"
        "\n"
        "Transcript:\n"
        f"{text}"
    )

    try:
        logging.info("STARTING Llama summarization (len=%d chars)", len(text))
        payload = {
            "model": LLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        }
        # Increased timeout to 120s for long transcripts
        result = call_llama_api(payload, timeout=120)

        if "response" in result:
            summary = result["response"].strip()
            logging.info(f"FINISHED Llama Summary: {len(summary)} chars")
            return summary
        else:
            logging.warning("Llama API returned unexpected format.")
            
    except Exception as e:
        logging.error(f"Llama summarization FAILED: {e}")
    
    # DO NOT FALLBACK TO TRANSCRIPT. User considers it "junk".
    logging.info("Returning failure message instead of junk fallback.")
    return "Error: Llama was unable to generate key points for this transcription. Please check the full transcript below."

def chunk_text_by_words(text, word_limit=2000):
    """
    Splits text into chunks of roughly word_limit words.
    """
    words = text.split()
    for i in range(0, len(words), word_limit):
        yield " ".join(words[i:i + word_limit])

def generate_topic_wise_consolidation(combined_text):
    """
    Synthesizes multiple transcriptions into a unique text structured by topics
    using a chunked Map-Reduce approach to handle large contexts.
    """
    if not combined_text:
        return ""
    
    # Pass 1: Extract insights from chunks
    chunks = list(chunk_text_by_words(combined_text, 2500))
    logging.info(f"Processing consolidation in {len(chunks)} chunks.")
    
    intermediate_insights = []
    
    for i, chunk in enumerate(chunks):
        logging.info(f"Processing chunk {i+1}/{len(chunks)}...")
        prompt = (
            "You are an analytical assistant. Below is a segment of transcription text.\n"
            "Extract all unique facts, technical details, specific data points, and key perspectives.\n"
            "Organize these insights clearly by topic. Avoid generic descriptions; focus on high-depth, unique 'meat' from this specific segment.\n\n"
            f"TEXT SEGMENT:\n{chunk}"
        )
        
        try:
            payload = {"model": LLAMA_MODEL, "prompt": prompt, "stream": False}
            result = call_llama_api(payload, timeout=120)
            intermediate_insights.append(result["response"].strip())

        except Exception as e:
            logging.error(f"Error processing chunk {i+1}: {e}")
            intermediate_insights.append(f"[Error processing segment {i+1}]")

    if not intermediate_insights:
        return "Error: Unable to process transcripts."

    # Pass 2: Final Synthesis
    all_insights = "\n\n--- SEGMENT INSIGHTS ---\n\n".join(intermediate_insights)
    logging.info("Starting final synthesis of all insights...")
    
    final_prompt = (
        "You are an expert content synthesizer. You have been given high-depth topic insights extracted from multiple video transcriptions.\n\n"
        "Your task is to synthesize these into a structured report segregated into DYNAMIC CATEGORIES derived from the content itself.\n\n"
        "Rules:\n"
        "1. Identify the major themes, topics, or stories present across all transcriptions.\n"
        "2. Create unique, descriptive category headers for each theme using the format: ## Category: [Theme Name]\n"
        "3. Provide a detailed, cohesive synthesis under each category. Use bold sub-headings and bullet points where appropriate.\n"
        "4. NO REPETITION: Ensure that each fact, data point, or insight is mentioned ONLY ONCE in the entire report. Assign information to the single most relevant category.\n"
        "5. DISTINCT CATEGORIES: Ensure categories do not overlap in scope. If two themes are closely related, merge them into one comprehensive category.\n"
        "6. Do NOT use generic headers like 'Interviews' or 'General News' unless they are perfectly descriptive of the content. Be specific to the actual subjects discussed.\n"
        "7. Maintain a professional, expert tone and weave all inputs into a single narrative for that category (no 'video 1' references).\n"
        "8. Provide 3-5 distinct, high-value categories if the content allows.\n\n"
        f"DATA TO SYNTHESIZE:\n{all_insights}"
    )

    try:
        payload = {"model": LLAMA_MODEL, "prompt": final_prompt, "stream": False}
        result = call_llama_api(payload, timeout=240)
        return result["response"].strip()

    except Exception as e:
        logging.error(f"Final synthesis failed: {e}")
    
    return "Error: Unable to generate final synthesized report."

def generate_unique_text(combined_text):
    """
    Synthesizes multiple transcriptions into a single unique cohesive article
    using a chunked Map-Reduce approach to handle large contexts.
    """
    if not combined_text:
        return ""
    
    # Pass 1: Extract insights from chunks (Map Phase)
    # Using 2500 word chunks to stay within safe context limits
    chunks = list(chunk_text_by_words(combined_text, 2500))
    logging.info(f"Processing unique text generation in {len(chunks)} chunks.")
    
    intermediate_insights = []
    
    for i, chunk in enumerate(chunks):
        logging.info(f"Processing chunk {i+1}/{len(chunks)} for unique text...")
        prompt = (
            "You are an analytical assistant. Below is a segment of transcription text from a larger collection. "
            "Analyze this segment and extract all key narratives, specific facts, data points, and unique perspectives. "
            "Do not just summarize; extract the 'meat' of the content that would be valuable for a deep-dive article.\n\n"
            f"TEXT SEGMENT:\n{chunk}"
        )
        
        try:
            payload = {"model": LLAMA_MODEL, "prompt": prompt, "stream": False}
            result = call_llama_api(payload, timeout=120)
            intermediate_insights.append(result["response"].strip())

        except Exception as e:
            logging.error(f"Error processing chunk {i+1}: {e}")
            intermediate_insights.append(f"[Error processing segment {i+1}]")

    if not intermediate_insights:
        return "Error: Unable to process transcriptions for unique text."

    # Pass 2: Final Synthesis (Reduce Phase)
    all_insights = "\n\n--- SEGMENT INSIGHTS ---\n\n".join(intermediate_insights)
    logging.info("Starting final synthesis of unique text...")
    
    final_prompt = (
        "You are an expert content creator. You have been given detailed insights extracted from multiple video transcriptions. "
        "Your goal is to synthesize these into ONE unique, highly detailed, and cohesive deep-dive article. "
        "This should not be a summary, but a new, standalone piece of professional content that integrates all inputs.\n\n"
        "Structure of the Article:\n"
        "1. Introduction: Set the stage and explain the combined theme.\n"
        "2. Comprehensive Analysis: A deep integration of all points, organized by logical themes/narratives. Weave the insights together.\n"
        "3. Final Insights: A high-impact takeaway and future outlook based on the data.\n\n"
        "Rules:\n"
        "- Use sophisticated, engaging, and professional language.\n"
        "- Ensure the flow is natural and transitions are smooth.\n"
        "- Avoid bullet points unless listing specific data points. Write in paragraphs.\n"
        "- Do NOT use the word 'Summary'.\n\n"
        f"DATA TO SYNTHESIZE:\n{all_insights}"
    )

    try:
        logging.info("STARTING Llama Synthesis (len=%d chars)", len(all_insights))
        payload = {
            "model": LLAMA_MODEL,
            "prompt": final_prompt,
            "stream": False
        }
        # Increased timeout for final large synthesis
        result = call_llama_api(payload, timeout=240)
        return result["response"].strip()

            
    except Exception as e:
        logging.error(f"Llama Unique Text Synthesis FAILED: {e}")
    
    return "Error: Unable to synthesize the unique text at this time."

if __name__ == "__main__":
    # Simple CLI test
    import sys
    logging.basicConfig(level=logging.INFO)
    test_text = "This is a test transcript for the summarization service."
    print("Testing summarization...")
    result = summarize_text(test_text)
    print("\nResult:")
    print(result)
