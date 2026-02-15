
import { GoogleGenAI } from "@google/genai";
import { NewsArticle } from "../types";

// NOTE: This service runs on the Client. 
// Do NOT use process.env.API_KEY here anymore.
// To use NewsDetail, the user must provide a key or we must route via backend.
// For this strict implementation, we default to empty or require input.

const parseJsonFromText = (text: string | undefined): any => {
  if (!text) return null;
  try {
    const cleanText = text.replace(/```json|```/g, '').trim();
    const startIndex = cleanText.indexOf('{');
    const arrayStart = cleanText.indexOf('[');
    if (arrayStart !== -1 && (startIndex === -1 || arrayStart < startIndex)) {
      return JSON.parse(cleanText.substring(arrayStart, cleanText.lastIndexOf(']') + 1));
    }
    return JSON.parse(cleanText.substring(startIndex, cleanText.lastIndexOf('}') + 1));
  } catch (e) { return null; }
};

const extractSources = (response: any): { title: string; uri: string }[] => {
  const sources: { title: string; uri: string }[] = [];
  const chunks = response.candidates?.[0]?.groundingMetadata?.groundingChunks;
  if (chunks) {
    chunks.forEach((chunk: any) => {
      if (chunk.web) sources.push({ title: chunk.web.title || 'Sumber Web', uri: chunk.web.uri });
    });
  }
  return sources;
};

// --- NEWS DETAIL SERVICE ---
// This is the only client-side AI left. It should ideally be moved to backend too.
// For now, it fails safely if no key is provided.

export const fetchNewsDetail = async (headline: string, contextUrl?: string): Promise<NewsArticle | null> => {
  // Check if user has stored a personal key, otherwise fail
  // We removed process.env.API_KEY to prevent leaks.
  // In a real production app, this call would go to your /internal/news-detail endpoint.
  const userKey = localStorage.getItem("VELRA_USER_API_KEY"); 
  
  if (!userKey) {
    console.warn("News Detail requires an API Key. Please implement a backend proxy or set VELRA_USER_API_KEY in localStorage for dev.");
    return null;
  }

  const ai = new GoogleGenAI({ apiKey: userKey });
  const key = `velra_detail_v3_${headline.substring(0,15).replace(/\W/g,'_')}`;
  
  try {
    const cached = localStorage.getItem(key);
    if (cached) return JSON.parse(cached);
  } catch(e) {}

  const prompt = `
    ROLE: Senior Financial Editor.
    TASK: Rewrite news from this URL: "${contextUrl || 'Search for: ' + headline}".
    OUTPUT JSON: { "headline": "Title", "author": "Velra Desk", "timestamp": "Today", "takeaways": ["Point 1"], "content": "Body..." }
  `;
  try {
    const res = await ai.models.generateContent({
        model: "gemini-3-flash-preview", 
        contents: prompt, 
        config: { tools: [{ googleSearch: {} }] }
    });
    const data = parseJsonFromText(res.text);
    if (data) {
      data.sources = extractSources(res);
      localStorage.setItem(key, JSON.stringify(data));
    }
    return data;
  } catch (error) { 
    console.error("News Detail Gen Failed:", error);
    return null; 
  }
};
