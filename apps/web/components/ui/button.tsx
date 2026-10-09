import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md border text-sm font-medium transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#d0d4d8] disabled:pointer-events-none disabled:opacity-40",
  {
    variants: {
      variant: {
        default: "border-[#3a4046] bg-[#23282d] text-[#e8eaed] hover:bg-[#2c333a]",
        quiet: "border-transparent bg-transparent text-[#c5cad0] hover:bg-[#23282d]",
        positive: "border-[#1f6b45] bg-[#143526] text-[#b7f0d2] hover:bg-[#184230]",
      },
      size: {
        default: "h-8 px-3",
        sm: "h-7 px-2 text-xs",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export function Button({ className, variant, size, asChild = false, ...props }: ButtonProps) {
  const Comp = asChild ? Slot : "button";
  return <Comp className={cn(buttonVariants({ variant, size }), className)} {...props} />;
}
