(function () {
    const MAX_DIMENSION = 1600;
    const JPEG_QUALITY = 0.82;
    const MIN_SIZE_BYTES = 250 * 1024;

    function supportsRequiredApis() {
        return !!(window.File && window.FileReader && window.URL && window.DataTransfer);
    }

    function isImageFile(file) {
        return !!(file && typeof file.type === "string" && file.type.startsWith("image/"));
    }

    function loadImageFromFile(file) {
        return new Promise((resolve, reject) => {
            const image = new Image();
            const url = URL.createObjectURL(file);

            image.onload = () => {
                URL.revokeObjectURL(url);
                resolve(image);
            };

            image.onerror = () => {
                URL.revokeObjectURL(url);
                reject(new Error("Nao foi possivel carregar a imagem."));
            };

            image.src = url;
        });
    }

    async function compressImageFile(file) {
        if (!isImageFile(file) || file.size < MIN_SIZE_BYTES) {
            return file;
        }

        try {
            const image = await loadImageFromFile(file);
            const ratio = Math.min(1, MAX_DIMENSION / Math.max(image.width, image.height));
            const width = Math.max(1, Math.round(image.width * ratio));
            const height = Math.max(1, Math.round(image.height * ratio));

            const canvas = document.createElement("canvas");
            canvas.width = width;
            canvas.height = height;

            const context = canvas.getContext("2d", { alpha: false });
            if (!context) {
                return file;
            }

            context.drawImage(image, 0, 0, width, height);

            const blob = await new Promise((resolve) => {
                canvas.toBlob(resolve, "image/jpeg", JPEG_QUALITY);
            });

            if (!blob || blob.size >= file.size) {
                return file;
            }

            const baseName = (file.name || "foto").replace(/\.[^.]+$/, "") || "foto";
            return new File([blob], `${baseName}.jpg`, {
                type: "image/jpeg",
                lastModified: Date.now(),
            });
        } catch (error) {
            return file;
        }
    }

    async function optimizeInputFiles(input) {
        if (!supportsRequiredApis() || !input || !input.files || input.dataset.compressing === "1") {
            return;
        }

        const originalFiles = Array.from(input.files);
        if (!originalFiles.length || !originalFiles.some(isImageFile)) {
            return;
        }

        input.dataset.compressing = "1";
        const previousTitle = input.title;
        input.title = "Otimizando imagens para upload...";

        try {
            const transfer = new DataTransfer();

            for (const file of originalFiles) {
                transfer.items.add(await compressImageFile(file));
            }

            input.files = transfer.files;
        } catch (error) {
            // Mantem os arquivos originais se o navegador nao deixar substituir a lista.
        } finally {
            input.dataset.compressing = "0";
            input.title = previousTitle || "";
        }
    }

    document.addEventListener("change", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) {
            return;
        }

        if (target.type !== "file" || !target.accept || !target.accept.includes("image/")) {
            return;
        }

        optimizeInputFiles(target);
    });
})();
