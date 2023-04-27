use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Entry)]
pub fn entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct we're deriving Entry for
    let name = &ast.ident;
    // Generate the implementation of Entry using the quote! macro
    let gen = quote! {
        impl ::omnipaxos_core::storage::Entry for #name
        {
            type Snapshot = ::omnipaxos_core::storage::NoSnapshot;
        }
    };

    // Convert the generated code back into tokens and return them
    gen.into()
}
