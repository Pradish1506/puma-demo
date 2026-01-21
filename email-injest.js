
/* -------------------------
   CONFIG
--------------------------*/

// ❗ MOVE THESE TO ENV VARS IN PROD
const TENANT_ID = "7e1d931c-a318-4d9d-8472-62e2437de1b0";
const CLIENT_ID = "89f6a458-fc26-4cb5-9e1b-ee045588c093";
const CLIENT_SECRET = "o~k8Q~PdbqWFkGMy898zFq5bE_gyaFzWHdWy3dt2"; // ROTATE THIS

const MAILBOX = "support@puma.quantaops.com";

const API_ENDPOINT =
  "https://puma-backend-demo-production.up.railway.app/email-inbox";

/* -------------------------
   MICROSOFT GRAPH
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

  if (!data.access_token) {
    throw new Error("Failed to get token: " + JSON.stringify(data));
  }

  return data.access_token;
}

async function fetchUnreadEmails() {
  const token = await getAccessToken();

  const res = await fetch(
    `https://graph.microsoft.com/v1.0/users/${MAILBOX}/mailFolders/inbox/messages?$filter=isRead eq false`,
    {
      headers: { Authorization: `Bearer ${token}` },
    }
  );

  const data = await res.json();

  if (!res.ok) {
    throw new Error("Graph error: " + JSON.stringify(data));
  }

  return data.value || [];
}

/* -------------------------
   SEND TO RAILWAY API
--------------------------*/
async function sendEmailToAPI(mail) {
  const payload = {
    message_id: mail.id,
    internet_message_id: mail.internetMessageId,

    from_name: mail.from?.emailAddress?.name || null,
    from_email: mail.from?.emailAddress?.address || null,
    to_email: MAILBOX,

    subject: mail.subject || null,
    body_preview: mail.bodyPreview || null,
    body_html: mail.body?.content || null,

    received_at: mail.receivedDateTime,

    channel: "email",
    processing_status: "new",
    linked_case_id: null,

    raw_payload: mail,
  };

  const res = await fetch(API_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error("API POST failed: " + err);
  }

  return res.json();
}

/* -------------------------
   MAIN FLOW
--------------------------*/
async function ingestEmails() {
  try {
    const mails = await fetchUnreadEmails();

    if (!mails.length) {
      console.log(" No new emails");
      return;
    }

    for (const mail of mails) {
      try {
        await sendEmailToAPI(mail);

        console.log(" Email sent to API:", {
          subject: mail.subject,
          from: mail.from?.emailAddress?.address,
          received: mail.receivedDateTime,
        });
      } catch (e) {
        console.error(" Failed for email:", mail.id, e.message);
      }
    }

    console.log(` Sent ${mails.length} emails to Railway API`);
  } catch (err) {
    console.error(" Ingestion failed:", err.message);
  }
}

/* -------------------------
   RUN
--------------------------*/
ingestEmails();
