// dashboard/components/UploadForm.tsx
// Drop-in replacement that works with the new index.tsx contract.
// It asks the parent for { jobId, upload(file) } and handles file selection/drag-drop.

import React, { useCallback, useRef, useState } from "react";

type OnJobCreated = (file: File | Blob) => Promise<{
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
        const { upload } = await onJobCreated(file);

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
    <div className="panel upload-panel">
      <div className="panel-heading">
        <p className="panel-label">Launch a run</p>
        <h2>Upload a new dataset</h2>
      </div>
      <p className="panel-description">
        Drag a CSV into the portal and we’ll handle the presigned upload, kick off processing, and surface live status
        as the job progresses.
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
          <div className="dz-copy">
            <div className="dz-title">Drop CSV to ignite</div>
            <div className="dz-sub">or click to browse files</div>
          </div>
        )}
        {busy && (
          <div className="dz-status">
            <span className="spinner" aria-hidden />
            <span>{localMsg}</span>
          </div>
        )}
      </div>

      <style jsx>{`
        .upload-panel {
          display: grid;
          gap: 1rem;
        }
        .dropzone {
          position: relative;
          border-radius: 22px;
          padding: 2.25rem 1.75rem;
          text-align: center;
          cursor: pointer;
          border: 1px dashed rgba(224, 203, 168, 0.45);
          background: radial-gradient(circle at top, rgba(136, 73, 143, 0.25), rgba(119, 159, 161, 0.08));
          transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
        }
        .dropzone::after {
          content: "";
          position: absolute;
          inset: 12px;
          border-radius: 18px;
          border: 1px solid rgba(255, 101, 66, 0.22);
          opacity: 0;
          transition: opacity 0.2s ease;
          pointer-events: none;
        }
        .dropzone:hover,
        .dropzone:focus-visible {
          transform: translateY(-2px);
          box-shadow: 0 28px 45px -38px rgba(255, 101, 66, 0.8);
          border-color: rgba(255, 101, 66, 0.55);
        }
        .dropzone:hover::after,
        .dropzone:focus-visible::after {
          opacity: 1;
        }
        .dropzone.disabled {
          opacity: 0.6;
          cursor: not-allowed;
          box-shadow: none;
        }
        .dz-copy {
          display: grid;
          gap: 0.45rem;
          color: #e0cba8;
        }
        .dz-title {
          font-size: 1.15rem;
          font-weight: 700;
          letter-spacing: 0.04em;
        }
        .dz-sub {
          font-size: 0.9rem;
          color: rgba(224, 203, 168, 0.65);
        }
        .dz-status {
          display: inline-flex;
          gap: 0.75rem;
          align-items: center;
          justify-content: center;
          font-size: 0.95rem;
          color: #e0cba8;
        }
        .spinner {
          width: 18px;
          height: 18px;
          border-radius: 50%;
          border: 2px solid rgba(255, 101, 66, 0.5);
          border-top-color: rgba(255, 101, 66, 0.9);
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
