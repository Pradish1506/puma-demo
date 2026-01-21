import fetch from "node-fetch";
import { detectIntent } from "./engines/intentEngine.js";
import { detectRisk } from "./engines/riskEngine.js";
import { decideRoute } from "./engines/decisionEngine.js";

/* -------------------------
   CONFIG
--------------------------*/
const TENANT_ID = "7e1d931c-a318-4d9d-8472-62e2437de1b0";
const CLIENT_ID = "89f6a458-fc26-4cb5-9e1b-ee045588c093";
const CLIENT = process.env.CLIENT_SECRET;
const MAILBOX = "support@puma.quantaops.com";
// Backend API URL (default to localhost if not set)
const API_URL = process.env.API_URL || "http://localhost:8000";

/* -------------------------
   API HELPERS
--------------------------*/
async function apiCall(endpoint, method, body) {
  try {
    const options = {
      method,
      headers: { "Content-Type": "application/json" },
    };
    if (body) options.body = JSON.stringify(body);

    const res = await fetch(`${API_URL}${endpoint}`, options);

    if (!res.ok) {
      const err = await res.text();
      console.error(`❌ API Error [${method} ${endpoint}]:`, err);
      return null;
    }

    return res.json();
  } catch (e) {
    console.error(`❌ API Network Error [${method} ${endpoint}]:`, e.message);
    return null;
  }
}

async function fetchCustomerOrders(email) {
  return await apiCall(`/orders?email=${encodeURIComponent(email)}`, "GET");
}

async function fetchOrderById(orderId) {
  return await apiCall(`/orders/${orderId}`, "GET");
}

/* -------------------------
   GRAPH API HELPERS
--------------------------*/
async function getAccessToken() {
  const res = await fetch(
    `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        scope: "https://graph.microsoft.com/.default",
        grant_type: "client_credentials",
      }),
    }
  );
  const data = await res.json();
  if (!data.access_token) throw new Error("Failed to get token");
  return data.access_token;
}

async function fetchUnreadEmails() {
  const token = await getAccessToken();
  const res = await fetch(
    `https://graph.microsoft.com/v1.0/users/${MAILBOX}/mailFolders/inbox/messages?$filter=isRead eq false`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json();
  if (!res.ok) throw new Error("Graph error");
  return data.value || [];
}

/* -------------------------
   ✅ REPLY (NOT SEND NEW)
--------------------------*/
async function sendReply(messageId, body) {
  const token = await getAccessToken();

  const payload = {
    comment: body, // HTML supported
  };

  const res = await fetch(
    `https://graph.microsoft.com/v1.0/users/${MAILBOX}/messages/${messageId}/reply`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    }
  );

  if (!res.ok) {
    const t = await res.text();
    throw new Error("Failed to reply: " + t);
  }

  console.log(`↩️ Replied to message ${messageId}`);
  return true;
}

/* -------------------------
   HELPERS
--------------------------*/
function extractOrderIds(text = "") {
  return text.match(/\b\d{5,}\b/g) || [];
}

/* -------------------------
   EMAIL TEMPLATES (Aligned with L1 Automation Doc)
--------------------------*/
const templates = {
  // --- 1. Information Seeking ---
  ask_order_id: () => `
Hello,<br><br>
Thank you for reaching out to Puma Support.<br>
To assist you better, we need your <b>Order ID</b> (e.g., PUMA-123456).<br><br>
Please reply with the correct Order ID so we can quickly check the status for you.<br><br>
Regards,<br>Puma Support
`,

  multiple_orders_found: (orders) => {
    const rows = orders.map(o =>
      `<tr>
         <td style="border: 1px solid #ddd; padding: 8px;">${o.order_id}</td>
         <td style="border: 1px solid #ddd; padding: 8px;">${o.items || 'Items'}</td>
         <td style="border: 1px solid #ddd; padding: 8px;">${o.status}</td>
         <td style="border: 1px solid #ddd; padding: 8px;">${o.created_at}</td>
       </tr>`
    ).join("");

    return `
Hello,<br><br>
We noticed you have multiple recent orders with us. To assist you correctly, please reply with the specific <b>Order ID</b> from the list below:<br><br>
<table style="border-collapse: collapse; width: 100%;">
  <thead>
    <tr style="background-color: #f2f2f2;">
      <th style="border: 1px solid #ddd; padding: 8px;">Order ID</th>
      <th style="border: 1px solid #ddd; padding: 8px;">Items</th>
      <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
      <th style="border: 1px solid #ddd; padding: 8px;">Date</th>
    </tr>
  </thead>
  <tbody>
    ${rows}
  </tbody>
</table>
<br>
Once you confirm the ID, we will proceed immediately.<br><br>
Regards,<br>Puma Support
`;
  },

  // --- 2. Order Status (FCR) ---
  order_created: (id) => `
Hello,<br><br>
Your order <b>${id}</b> is confirmed! 🎉<br>
It normally takes <b>1-2 business days</b> to pack and dispatch your items.<br>
You will receive a notification as soon as it ships.<br><br>
Regards,<br>Puma Support
`,

  order_packed: (id) => `
Hello,<br><br>
Good news! Your order <b>${id}</b> has been packed and is ready for pickup by our courier partner.<br>
It should ship within the next 24 hours.<br><br>
Regards,<br>Puma Support
`,

  order_shipped: (id, link = "#") => `
Hello,<br><br>
Your order <b>${id}</b> is on the way! 🚚<br>
You can track your package here: <a href="${link}">Track Order</a><br><br>
Expected delivery is within 3-5 days.<br><br>
Regards,<br>Puma Support
`,

  delivery_attempt_failed: (id) => `
Hello,<br><br>
We noticed a failed delivery attempt for your order <b>${id}</b>.<br>
Don't worry, our courier partner will attempt delivery again on the next business day.<br>
Please ensure someone is available to receive the package.<br><br>
Regards,<br>Puma Support
`,

  order_delivered: (id) => `
Hello,<br><br>
Our records show that your order <b>${id}</b> has been delivered.<br>
If you haven't received it, please let us know immediately.<br><br>
Regards,<br>Puma Support
`,

  order_returned: (id) => `
Hello,<br><br>
We have received your return for order <b>${id}</b>.<br>
Your refund is being processed and should reflect within 5-7 business days.<br><br>
Regards,<br>Puma Support
`,

  // --- 3. Order Status (Agent Handoff / Exceptions) ---
  agent_handoff_generic: (id) => `
Hello,<br><br>
We are looking into your query${id ? ` regarding order <b>${id}</b>` : ""}.<br>
One of our support specialists has been assigned to your case and will revert with an update shortly.<br><br>
Thank you for your patience.<br><br>
Regards,<br>Puma Support
`,

  agent_handoff_stuck: (id) => `
Hello,<br><br>
We apologize for the delay with order <b>${id}</b>.<br>
We have escalated this to our logistics team to investigate the movement status.<br>
You will hear from us soon with a resolution.<br><br>
Regards,<br>Puma Support
`,

  // --- 4. Cancellation & corrections ---
  cancellation_whatsapp: () => `
Hello,<br><br>
To cancel your order instantly, please use our automated WhatsApp service:<br><br>
👉 <a href="https://wa.me/puma_support?text=cancel"><b>Click here to Cancel Order on WhatsApp</b></a><br><br>
(Note: Cancellation is only possible before the order is shipped.)<br><br>
Regards,<br>Puma Support
`,

  address_change_denied: () => `
Hello,<br><br>
We understand you wish to change your delivery address.<br>
Currently, our system **does not support address changes** once an order is placed due to security/logistics constraints.<br><br>
Please coordinate directly with the courier partner once you receive the delivery SMS.<br><br>
Regards,<br>Puma Support
`,

  // --- 5. Refunds (FCR) ---
  refund_in_sla: (id) => `
Hello,<br><br>
Your refund for order <b>${id}</b> has been initiated.<br>
<b>Timeline:</b> Refunds are initiated within 3 days of return pickup and typically reflect in your bank account within **5–7 business days** after that.<br><br>
If you do not see the credit by then, please let us know.<br><br>
Regards,<br>Puma Support
`,

  refund_processed: (id, arn = "N/A") => `
Hello,<br><br>
Your refund for order <b>${id}</b> has been successfully processed.<br>
<b>Bank Reference (ARN):</b> ${arn}<br><br>
Please check your bank statement. If not visible, contact your bank with this ARN number.<br><br>
Regards,<br>Puma Support
`,

  // --- 6. Refunds (Agent Handoff) ---
  refund_issue_handoff: (id) => `
Hello,<br><br>
We apologize for the delay in your refund for order <b>${id}</b>.<br>
We have assigned this to our Finance Team to verify the transaction status.<br>
We will update you as soon as we have confirmation.<br><br>
Regards,<br>Puma Support
`,

  // --- 7. Risk / Other ---
  high_risk_escalation: () => `
Hello,<br><br>
We have received your email and it has been flagged for prioritized review.<br>
A Senior Support Specialist will be assessing your concern and will contact you directly.<br><br>
Regards,<br>Puma Support
`,

  unclear_intent: () => `
Hello,<br><br>
We definitely want to help, but we didn't fully understand your request.<br>
Could you please share more details or your Order ID so we can assist you?<br><br>
Regards,<br>Puma Support
`,

  invoice_shared: (id) => `
Hello,<br><br>
We have triggered a request for your invoice for order <b>${id}</b>.<br>
It will be sent to your registered email address shortly.<br><br>
Regards,<br>Puma Support
`
};

/* -------------------------
   TEMPLATE DECIDER
--------------------------*/
function buildReply({ intent, risk, confidence, orderIds, decision, suggestedOrder, multipleOrders, orderData }) {
  // 1. Risk Override
  if (risk) return templates.high_risk_escalation();

  // 2. Multiple Orders Found -> Ask user to choose
  if (multipleOrders && multipleOrders.length > 1) {
    return templates.multiple_orders_found(multipleOrders);
  }

  // 3. Missing Order ID check
  // Uses inferred ID if available
  const activeOrderId = orderIds[0] || suggestedOrder;

  // Force "Ask Order ID" for any intent that typically requires it, AND for generic unclear queries if no ID found
  // Updated list to include generic inquiries that might be order-related
  const intentsNeedingId = [
    "order_status",
    "refund_not_received",
    "invoice_request",
    "report_problem",
    "delivery_issue",
    "payment_issue"
  ];

  const needsOrderId = intentsNeedingId.includes(intent);

  if (!activeOrderId) {
    if (needsOrderId) return templates.ask_order_id();

    // Logic: If intent is unknown/generic but confidence is low or it looks like a complaint, 
    // it's safer to ask for Order ID than to send a generic "Agent Assigned" message without context.
    if (intent === "unknown" || decision?.owner === "agent") {
      // Optional: You can decide to ask for ID here too. 
      // For now, let's allow generic agent handoff but WITHOUT the invalid ID.
    }
  }

  const id = activeOrderId || ""; // Default to empty string if missing, so template handles it
  const isAgentHandoff = decision?.owner === "agent" || decision?.owner === "senior_support";

  // 4. Intent Routing
  switch (intent) {
    case "order_status":
      if (isAgentHandoff) return templates.agent_handoff_stuck(id || "YOUR_ORDER");

      // Dynamic Status Check
      const status = orderData?.status?.toLowerCase() || "processing";
      if (status === "created") return templates.order_created(id);
      if (status === "packed") return templates.order_packed(id);
      if (status === "delivered") return templates.order_delivered(id);
      if (status === "returned") return templates.order_returned(id);

      return templates.order_shipped(id); // Default to Shipped (most common FCR)

    case "refund_not_received":
      if (isAgentHandoff) return templates.refund_issue_handoff(id || "YOUR_ORDER");
      return templates.refund_in_sla(id);

    case "cancellation_request":
      return templates.cancellation_whatsapp();

    case "address_change_request":
      return templates.address_change_denied();

    case "return_exchange_request":
      return templates.return_exchange();

    case "invoice_request":
      return templates.invoice_shared(id);

    case "report_problem":
    case "payment_issue":
      return templates.agent_handoff_generic(id);

    // Fallback
    default:
      if (confidence < 0.7) return templates.unclear_intent();
      return templates.agent_handoff_generic(id);
  }
}

/* -------------------------
   WORKER
--------------------------*/
async function processEmails() {
  try {
    const emails = await fetchUnreadEmails();
    if (!emails.length) return console.log("📭 No new emails");

    for (const email of emails) {
      const emailId = email.id;
      if (!emailId) continue;

      try {
        console.log(`🔹 Processing email: ${email.subject}`);

        // 0. Extract Sender Email
        const senderEmail = email.from?.emailAddress?.address;

        // 1. Injest Email to DB
        const savedEmail = await apiCall("/email-inbox", "POST", {
          message_id: email.id,
          internet_message_id: email.internetMessageId,
          from_name: email.from?.emailAddress?.name,
          from_email: senderEmail,
          to_email: MAILBOX,
          subject: email.subject,
          body_preview: email.bodyPreview,
          body_html: email.body?.content,
          received_at: email.receivedDateTime,
          channel: "email",
          processing_status: "processing",
          raw_payload: email,
        });

        if (!savedEmail) {
          console.warn("Skipping processing as email ingest failed.");
          continue;
        }

        // 2. AI Engines
        const intentRes = await detectIntent(email);
        const riskRes = await detectRisk(email);

        const intent = intentRes.intent || "unknown";
        const confidence = Number(intentRes.confidence || 0.1);
        const risk = Boolean(riskRes.risk);

        const decision = await decideRoute({ intent, confidence, risk });

        console.log(`   🔸 Intent: ${intent} | Risk: ${risk} | Decision: ${decision.status}`);

        // 3. Extract Order IDs
        const text = `${email.subject || ""} ${email.bodyPreview || ""}`;
        let orderIds = extractOrderIds(text);

        // --- ORDER ID INFERENCE START ---
        let suggestedOrder = null;
        let multipleOrders = null;

        if (orderIds.length === 0 && senderEmail) {
          const customerOrders = await fetchCustomerOrders(senderEmail);

          if (customerOrders && customerOrders.length === 1) {
            // Exact match found - auto use it
            suggestedOrder = customerOrders[0].order_id;
            console.log(`   ✅ Auto-inferred Order ID: ${suggestedOrder}`);
          } else if (customerOrders && customerOrders.length > 1) {
            // Multiple match - ask user
            multipleOrders = customerOrders;
            console.log(`   ⚠️ Multiple orders found: ${customerOrders.length}`);
          } else {
            console.log(`   ❌ No orders found for ${senderEmail}`);
          }
        }
        // --- ORDER ID INFERENCE END ---

        // 4. Create Case
        const casePayload = {
          salesforce_case_id: null,
          channel: "email",
          intent_type: intent,
          confidence_score: confidence,
          risk_flag: risk,
          status: decision.status,
          assigned_to: decision.owner,
        };

        const savedCaseResponse = await apiCall("/cases", "POST", casePayload);
        const caseId = savedCaseResponse?.data?.case_id;

        if (caseId) {
          await apiCall("/ai-decisions", "POST", {
            case_id: caseId,
            intent_detected: intent,
            confidence_score: confidence,
            decision_type: decision.status,
            reason_code: decision.owner,
            model_version: "v1.0"
          });

          if (risk) {
            await apiCall("/risk-events", "POST", {
              case_id: caseId,
              keyword_detected: riskRes.reason || "unknown",
              risk_level: "high",
              action_taken: "escalated"
            });
          }
        }

        // 5. Store Orders & Fetch Data
        const finalOrderId = orderIds[0] || suggestedOrder;
        let orderData = null;

        if (finalOrderId) {
          // Link to case
          if (caseId) {
            await apiCall("/case-orders", "POST", {
              case_id: caseId,
              order_id: finalOrderId,
              is_valid: true
            });
          }

          // Fetch real status for dynamic reply
          // If we already inferred it, we have it in multipleOrders/customerOrders usually, but let's be safe
          if (suggestedOrder && multipleOrders && multipleOrders.length === 1) {
            orderData = multipleOrders[0];
          } else {
            orderData = await fetchOrderById(finalOrderId);
          }
        }

        // 6. Build and Send Reply
        const replyBody = buildReply({
          intent,
          risk,
          confidence,
          orderIds,
          decision,
          suggestedOrder,
          multipleOrders,
          orderData // Pass the full order object
        });

        const replySent = await sendReply(emailId, replyBody);

        if (replySent && caseId) {
          await apiCall("/communications", "POST", {
            case_id: caseId,
            channel: "email",
            template_id: intent,
            message_status: "sent",
            sent_at: new Date().toISOString()
          });
        }

        await apiCall("/system-audit-logs", "POST", {
          entity_type: "email",
          entity_id: emailId.substring(0, 99),
          action: "processed",
          performed_by: "system",
          timestamp: new Date().toISOString()
        });

      } catch (err) {
        console.error(`Email ${emailId} failed:`, err.message);
      }
    }
  } catch (err) {
    console.error("Worker crashed:", err.message);
  }
}

/* -------------------------
   RUN
--------------------------*/
async function startWorker() {
  console.log("🚀 Email AI Worker started");
  console.log(`connects to: ${API_URL}`);

  while (true) {
    try {
      await processEmails();
    } catch (e) {
      console.error("Worker loop error:", e.message);
    }

    // ⏳ wait 30 seconds before next check
    await new Promise((r) => setTimeout(r, 30000));
  }
}

startWorker();
