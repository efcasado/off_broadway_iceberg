(() => {
  let sequence = 0;

  window.addEventListener("exdoc:loaded", () => {
    if (!window.mermaid) return;

    mermaid.initialize({
      startOnLoad: false,
      theme: document.body.classList.contains("dark") ? "dark" : "default"
    });

    for (const code of document.querySelectorAll("pre code.mermaid")) {
      const pre = code.parentElement;
      const graph = document.createElement("div");
      pre.replaceWith(graph);

      mermaid.render("iceberg-mermaid-" + sequence++, code.textContent)
        .then(({svg, bindFunctions}) => {
          if (!graph.isConnected) return;
          graph.innerHTML = svg;
          const image = graph.querySelector("svg");
          image.style.maxWidth = "100%";
          image.style.height = "auto";
          bindFunctions?.(graph);
        })
        .catch(error => {
          graph.replaceWith(pre);
          console.warn("Could not render Mermaid diagram:", error);
        });
    }
  });
})();
