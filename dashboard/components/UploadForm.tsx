// dashboard/components/UploadForm.tsx
// Drop-in replacement that works with the new index.tsx contract.
// It asks the parent for { jobId, upload(file) } and handles file selection/drag-drop.

import React, { useCallback, useRef, useState } from "react";

type OnJobCreated = () => Promise<{
  jobId: string;
  upload: (file: File | Blob) => Promise<void>;
}>;

export default function UploadForm({
  onJobCreated,
  onError,
}: {
  onJobCreated: OnJobCreated;
  onError?: (message: string) => void;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [busy, setBusy] = useState<false | "creating" | "uploading" | "done">(
    false,
  );
  const [localMsg, setLocalMsg] = useState<string>("");

  const pickFile = useCallback(() => inputRef.current?.click(), []);

  const handleFiles = useCallback(
    async (files: FileList | null) => {
      if (!files || !files[0]) return;
      const file = files[0];

      try {
        setBusy("creating");
        setLocalMsg("Creating job…");
        const { upload } = await onJobCreated();

        setBusy("uploading");
        setLocalMsg("Uploading dataset…");
        await upload(file);

        setBusy("done");
        setLocalMsg("Upload complete. Processing…");
      } catch (e) {
        const msg =
          e instanceof Error ? e.message : "Failed to upload and start processing";
        onError?.(msg);
        setBusy(false);
        setLocalMsg("");
      }
    },
    [onJobCreated, onError],
  );

  const onInputChange = useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      await handleFiles(e.target.files);
      // clear input so selecting the same file again still triggers change
      if (inputRef.current) inputRef.current.value = "";
    },
    [handleFiles],
  );

  const onDrop = useCallback(
    async (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.stopPropagation();
      await handleFiles(e.dataTransfer.files);
    },
    [handleFiles],
  );

  const prevent = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  return (
    <div className="glass-panel uploader">
      <p className="eyebrow">Upload</p>
      <h2>New dataset</h2>
      <p className="subtitle">
        Drop a CSV here and we’ll upload it, kick off processing, and render a user-friendly analysis.
      </p>

      <div
        className={`dropzone ${busy ? "disabled" : ""}`}
        onClick={busy ? undefined : pickFile}
        onDrop={busy ? undefined : onDrop}
        onDragOver={busy ? undefined : prevent}
        onDragEnter={busy ? undefined : prevent}
        onDragLeave={busy ? undefined : prevent}
        role="button"
        aria-disabled={!!busy}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".csv,text/csv"
          className="hidden"
          onChange={onInputChange}
        />
        {!busy && (
          <>
            <div className="dz-title">Drag & drop a CSV</div>
            <div className="dz-sub">or click to choose a file</div>
          </>
        )}
        {busy && (
          <div className="dz-status">
            <span className="spinner" aria-hidden />
            <span>{localMsg}</span>
          </div>
        )}
      </div>

      <style jsx>{`
        .uploader {
          display: grid;
          gap: 0.6rem;
        }
        .dropzone {
          border: 2px dashed rgba(0, 0, 0, 0.2);
          border-radius: 1rem;
          padding: 1.25rem;
          text-align: center;
          cursor: pointer;
          background: #fff;
        }
        .dropzone:hover {
          background: #fafafa;
        }
        .dropzone.disabled {
          opacity: 0.7;
          cursor: not-allowed;
        }
        .dz-title {
          font-size: 1.05rem;
          font-weight: 600;
        }
        .dz-sub {
          font-size: 0.9rem;
          opacity: 0.7;
        }
        .dz-status {
          display: inline-flex;
          gap: 0.6rem;
          align-items: center;
          justify-content: center;
          font-size: 0.95rem;
        }
        .spinner {
          width: 16px;
          height: 16px;
          border-radius: 50%;
          border: 2px solid #000;
          border-top-color: transparent;
          display: inline-block;
          animation: spin 0.9s linear infinite;
        }
        @keyframes spin {
          to {
            transform: rotate(360deg);
          }
        }
        .hidden {
          display: none;
        }
      `}</style>
    </div>
  );
}
