use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident};

/// Derive macro for declaring an OmniPaxos log entry type.
///
/// ## Usage
///
/// ```rust
/// use omnipaxos::macros::Entry;
/// #[derive(Clone, Debug, Entry)]
/// #[snapshot(KVSnapshot)] // KVSnapshot is a type that implements the Snapshot trait. Remove this if snapshot is not used.
/// pub struct KeyValue {
///     pub key: String,
///     pub value: u64,
/// }
/// ```
#[proc_macro_derive(Entry, attributes(snapshot))]
pub fn entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct we're deriving Entry for
    let name = &ast.ident;
    let snapshot_type = get_snapshot_type(&ast);
    // Generate the implementation of Entry using the quote! macro
    let gen = quote! {
        impl ::omnipaxos::storage::Entry for #name
        {
            type Snapshot = #snapshot_type;
        }
    };

    // Convert the generated code back into tokens and return them
    gen.into()
}

/// Derive macro for declaring an OmniPaxos log entry type that uses UniCache.
/// UniCache can reduce the amount of transmitted data by caching the values of user-specified fields and encoding them as smaller types.
/// For instance, a popular `String` that appears repeatedly can be sent over the network as an `u8`.
///
/// # Attributes
/// * `encoding(T)`: (Optional) The type for what the annotated field should be encoded as. The default is `u8`.
/// * `size(usize)`: (Optional) The size of the cache for this field. Should not be larger than the max size of the encoding type, e.g., if `encoding(u8)` is used, the max size should be 255.
/// The default value is `u8::MAX`.
/// * `cache(C)`: (Optional) The cache implementation which is a type `C: UniCache`. To use one of the provided implementations, simply use `cache(lru)` or `cache(lfu)`.
/// The default uses `lru` (least-recently-used) eviction policy.
///
/// ## Usage
///
/// ```rust
/// #[cfg(feature = "unicache")]
/// #[cfg_attr(feature = "unicache", derive(UniCacheEntry))]
/// #[derive(Clone, Debug, Default, PartialOrd, PartialEq, Serialize, Deserialize, Eq, Hash)]
/// pub struct Person {
///     pub id: u64,
///     #[unicache(encoding(u8))]   // First names can be cached e.g., if "John" repeatedly occurs in the log, it will be sent as a u8 instead.
///     first_name: String,
///     #[unicache(encoding(u64), size(100))]   // The size of the cache for this field can be optionally specified using the `size` attribute.
///     last_name: String,
///     #[unicache(encoding(u64), size(20), cache(lru))]    // use provided UniCache implementation with least-recently-used (LRU) eviction policy.
///     job: String,
/// }
///
/// ```
#[proc_macro_derive(UniCacheEntry, attributes(unicache, snapshot))]
pub fn unicache_entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let snapshot_type = get_snapshot_type(&ast);
    let mut cache_type = None;

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
            let mut cache_types = Vec::new();

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
                            // cache_type = Some(quote::quote!(#ty));
                            // let id = attr
                            //     .parse_args::<syn::Ident>()
                            //     .expect("Expected identifier");
                            cache_type = if ty == "lru" {
                                Some(quote!(::omnipaxos::unicache::lru_cache::LRUniCache))
                            } else if ty == "lfu" {
                                Some(quote!(::omnipaxos::unicache::lfu_cache::LFUniCache))
                            } else {
                                panic!("Invalid cache type")
                            };
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
                    cache_size = cache_size.take().or_else(|| Some(quote::quote!(u8::MAX as usize)));
                    cache_type = cache_type.take().or_else(|| Some(quote::quote!(::omnipaxos::unicache::lru_cache::LRUniCache)));

                    encodable_field_names.push(name);
                    encodable_field_attr_types.push(encoding_type.clone());
                    encodable_field_types.push(ty);
                    encode_result.push(quote!(MaybeEncoded<#ty, #encoding_type>));
                    cache_fields.push(quote!(#name: #cache_type<#ty, #encoding_type>));
                    cache_sizes.push(cache_size);
                    cache_types.push(cache_type.clone());
                } else {
                    non_encodable_field_names.push(name);
                    non_encodable_field_types.push(ty);
                    encode_result.push(quote!(#ty));
                }
            }

            let cache_name = suffix(name, "Cache");

            quote! {
                use ::omnipaxos::unicache::FieldCache;      // TODO remove this and use fully qualified path in the method calls instead?

                impl #impl_generics Entry for #name #ty_generics #where_clause {
                    type Snapshot = #snapshot_type;
                    type Encoded = (#(#encodable_field_attr_types,)*);
                    type Encodable = (#(#encodable_field_types,)*);
                    type NotEncodable = (#(#non_encodable_field_types,)*);
                    type EncodeResult = (#(#encode_result,)*);
                    type UniCache = #cache_name #ty_generics;
                }

                #[derive(Clone, Debug, Serialize, Deserialize)]
                pub struct #impl_generics #cache_name #ty_generics #where_clause {
                    #(#cache_fields,)*
                }

                impl #impl_generics UniCache for #cache_name #ty_generics #where_clause {
                    type T = #name #ty_generics;

                    fn new() -> Self {
                        Self {
                            #(#encodable_field_names: #cache_types::new(#cache_sizes),)*
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

fn get_snapshot_type(ast: &DeriveInput) -> quote::__private::TokenStream {
    let snapshot_type = ast
        .attrs
        .iter()
        .find_map(|attr| {
            if attr.path().is_ident("snapshot") {
                let t = attr.parse_args::<syn::Type>().expect("Expected type");
                Some(quote!(#t))
            } else {
                None
            }
        })
        .unwrap_or_else(|| quote!(::omnipaxos::storage::NoSnapshot));
    snapshot_type
}
