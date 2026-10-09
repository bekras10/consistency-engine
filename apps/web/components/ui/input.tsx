import * as React from "react";

import { cn } from "@/lib/utils";

export function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className={cn(
        "h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm text-[#e8eaed] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[#d0d4d8]",
        props.className,
      )}
    />
  );
}
