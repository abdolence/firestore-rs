// Shows each footnote's text as the hover title of its reference, so a note can be read in place.
document.querySelectorAll(".footnote-reference a[href^='#']").forEach((link) => {
  const note = document.getElementById(decodeURIComponent(link.hash.slice(1)));
  if (!note) return;
  const text = note.cloneNode(true);
  text.querySelectorAll("a[href^='#fr-']").forEach((backlink) => backlink.remove());
  link.title = text.textContent.replace(/\s+/g, " ").trim();
});
