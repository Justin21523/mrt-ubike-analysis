/**
 * @typedef {{tone?: 'primary'|'support'|'meta', kicker?: string, title: string, badge?: {tone?: string, text: string}, rightHtml?: string, actions?: Array<any>, bodyHtml?: string}} CardModel
 */

export function createCard({ tone = "support", kicker, title, badge, right, actions } = {}) {
  const card = document.createElement("section");
  card.className = `card ${tone}`;

  const header = document.createElement("div");
  header.className = "card-header";

  const left = document.createElement("div");
  left.className = "card-head-left";

  const kick = document.createElement("div");
  kick.className = "card-kicker";
  kick.textContent = kicker || "";

  const t = document.createElement("div");
  t.className = "card-title";
  t.textContent = title || "";

  left.appendChild(kick);
  left.appendChild(t);

  const headRight = document.createElement("div");
  headRight.className = "card-head-right";

  if (badge) {
    const b = document.createElement("span");
    b.className = `badge ${badge.tone || ""}`;
    b.textContent = badge.text || "";
    headRight.appendChild(b);
  }

  if (right) {
    const r = document.createElement("div");
    r.className = "card-right";
    r.innerHTML = right;
    headRight.appendChild(r);
  }

  header.appendChild(left);
  header.appendChild(headRight);

  const body = document.createElement("div");
  body.className = "card-body";

  card.appendChild(header);
  card.appendChild(body);

  if (Array.isArray(actions) && actions.length) {
    const row = document.createElement("div");
    row.className = "row row-actions card-actions";
    for (const a of actions) {
      if (a.type === "link") {
        const el = document.createElement("a");
        el.className = `btn ${a.primary ? "btn-primary" : ""}`;
        el.href = a.href;
        el.textContent = a.label;
        row.appendChild(el);
        continue;
      }
      const b = document.createElement("button");
      b.className = `btn ${a.primary ? "btn-primary" : ""}`;
      b.textContent = a.label;
      b.addEventListener("click", () => a.onClick?.());
      row.appendChild(b);
    }
    card.appendChild(row);
  }

  return { card, body };
}

/** @param {CardModel} model */
export function renderCard(model) {
  const { card, body } = createCard({
    tone: model.tone || "support",
    kicker: model.kicker || "",
    title: model.title,
    badge: model.badge || null,
    right: model.rightHtml || "",
    actions: model.actions || [],
  });
  if (model.bodyHtml != null) body.innerHTML = model.bodyHtml;
  return card;
}

