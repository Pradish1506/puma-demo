import fetch from "node-fetch";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

export async function callLLM(prompt) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      temperature: 0,
      messages: [{ role: "user", content: prompt }],
    }),
  });

  const data = await res.json();

  // 🔍 DEBUG if OpenAI sends error
  if (data.error) {
    console.error("❌ OpenAI API error:", data.error);
    throw new Error(data.error.message);
  }

  // ✅ SAFE extraction
  const text =
    data?.choices?.[0]?.message?.content ||
    "";

  if (!text) {
    console.error("❌ Empty LLM response:", JSON.stringify(data));
    throw new Error("LLM returned empty response");
  }

  return text;
}
