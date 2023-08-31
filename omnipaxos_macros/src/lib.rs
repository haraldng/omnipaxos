use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident};

/// Derive macro for declaring an OmniPaxos log entry type that does not use snapshots.
///
/// ## Usage
///
/// ```rust
/// use omnipaxos::macros::Entry;
/// #[derive(Clone, Debug, Entry)]
/// pub struct KeyValue {
///     pub key: String,
///     pub value: u64,
/// }
/// ```
// TODO add snapshot attribute
#[proc_macro_derive(Entry)]
pub fn entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct we're deriving Entry for
    let name = &ast.ident;
    // Generate the implementation of Entry using the quote! macro
    let gen = quote! {
        impl ::omnipaxos::storage::Entry for #name
        {
            type Snapshot = ::omnipaxos::storage::NoSnapshot;
        }
    };

    // Convert the generated code back into tokens and return them
    gen.into()
}

// TODO add snapshot attribute
#[proc_macro_derive(UniCacheEntry, attributes(unicache))]
pub fn unicache_entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let cache_type = ast
        .attrs
        .iter()
        .find_map(|attr| {
            if attr.path().is_ident("unicache") {
                let id = attr
                    .parse_args::<syn::Ident>()
                    .expect("Expected identifier");
                if id == "lru" {
                    Some(quote!(LRUniCache))
                } else if id == "lfu" {
                    Some(quote!(LFUniCache))
                } else {
                    panic!("Invalid cache type")
                }
            } else {
                None
            }
        })
        .unwrap_or_else(|| quote!(LRUniCache));

    match ast.data {
        syn::Data::Struct(data) => {
            let mut field_names = Vec::new();

            let mut encodable_field_names = Vec::new();
            let mut encodable_field_types = Vec::new();
            let mut encodable_field_attr_types = Vec::new();
            let mut encode_result = Vec::new();

            let mut non_encodable_field_names = Vec::new();
            let mut non_encodable_field_types = Vec::new();

            let mut cache_fields = Vec::new();
            let mut cache_sizes = Vec::new();

            for field in &data.fields {
                let name = field.ident.as_ref().unwrap();
                let ty = &field.ty;
                let attr = field.attrs.iter().find_map(|attr| {
                    if attr.path().is_ident("unicache") {
                        Some(attr)
                    } else {
                        None
                    }
                });
                field_names.push(name);
                if let Some(attr) = attr {
                    let mut encoding_type = None;
                    let mut cache_size = None;
                    let mut cache_type = None;
                    attr.parse_nested_meta(|meta| {
                        let path = meta.path;
                        if path.is_ident("size") {
                            let content;
                            syn::parenthesized!(content in meta.input);
                            let lit: syn::LitInt = content.parse()?;
                            let n: usize = lit.base10_parse()?;
                            cache_size = Some(quote::quote!(#n));
                        } else if path.is_ident("cache") {
                            let content;
                            syn::parenthesized!(content in meta.input);
                            let ty: syn::Ident = content.parse()?;
                            cache_type = Some(quote::quote!(#ty));
                        } else if path.is_ident("encoding") {
                            let content;
                            syn::parenthesized!(content in meta.input);
                            let ty: syn::Ident = content.parse()?;
                            encoding_type = Some(quote::quote!(#ty));
                        } else {
                            panic!("Found unexpected attribute `{}`", quote::quote!(#path))
                        }
                        Ok(())
                    }).expect(format!("Expected a valid attribute {}", quote::quote!(#attr)).as_str());
                    // Defaults
                    encoding_type = encoding_type.take().or_else(|| Some(quote!(u8)));
                    cache_size = cache_size.take().or_else(|| Some(quote::quote!(size)));
                    cache_type = cache_type.take().or_else(|| Some(quote::quote!(LRUniCache)));

                    encodable_field_names.push(name);
                    encodable_field_attr_types.push(encoding_type.clone());
                    encodable_field_types.push(ty);
                    encode_result.push(quote!(MaybeEncoded<#ty, #encoding_type>));
                    cache_fields.push(quote!(#name: #cache_type<#ty, #encoding_type>));
                    cache_sizes.push(cache_size);
                } else {
                    non_encodable_field_names.push(name);
                    non_encodable_field_types.push(ty);
                    encode_result.push(quote!(#ty));
                }
            }

            let cache_name = suffix(name, "Cache");

            quote! {
                impl #impl_generics Entry for #name #ty_generics #where_clause {
                    type Snapshot = ::omnipaxos::storage::NoSnapshot;
                    type Encoded = (#(#encodable_field_attr_types,)*);
                    type Encodable = (#(#encodable_field_types,)*);
                    type NotEncodable = (#(#non_encodable_field_types,)*);
                    type EncodeResult = (#(#encode_result,)*);
                    type UniCache = #cache_name #ty_generics;
                }

                #[derive(Clone, Debug, Serialize, Deserialize)]
                struct #impl_generics #cache_name #ty_generics #where_clause {
                    #(#cache_fields,)*
                }

                impl #impl_generics UniCache for #cache_name #ty_generics #where_clause {
                    type T = #name #ty_generics;

                    fn new(size: usize) -> Self {
                        Self {
                            #(#encodable_field_names: #cache_type::new(#cache_sizes),)*
                        }
                    }

                    fn try_encode(&mut self, entry: &Self::T) -> <Self::T as Entry>::EncodeResult {
                        #(let #encodable_field_names = self.#encodable_field_names.try_encode(&entry.#encodable_field_names);)*
                        #(let #non_encodable_field_names = entry.#non_encodable_field_names.clone();)*
                        (#(#field_names,)*)
                    }

                    fn decode(&mut self, (#(#field_names,)*): <Self::T as Entry>::EncodeResult) -> Self::T {
                        Self::T {
                            #(#encodable_field_names: self.#encodable_field_names.decode(#encodable_field_names),)*
                            #(#non_encodable_field_names,)*
                        }
                    }
                }
            }
        }
        syn::Data::Enum(_data) => {
            todo!()
        }
        syn::Data::Union(..) => unreachable!(),
    }
    .into()
}

fn suffix(ident: &Ident, name: &str) -> syn::Ident {
    syn::Ident::new(format!("{}{}", ident, name).as_str(), ident.span())
}