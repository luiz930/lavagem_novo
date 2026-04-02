self.addEventListener("install", e => {
    console.log("Service Worker instalado");
});

self.addEventListener("fetch", e => {
    // Pode evoluir depois com cache
});