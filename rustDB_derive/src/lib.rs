#![allow(non_snake_case)]
use proc_macro::TokenStream;
use quote::quote;

fn impl_operate_macro(input: &syn::DeriveInput) -> TokenStream {
    let name = &input.ident;
    let syn::Data::Enum(data) = &input.data else {panic!()};
    let operate_next_fields = data.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        quote!(
            Self::#variant_name(e) => e.next(buf)
        )
    });
    let operate_next = quote!(
        fn next(&mut self, buf: &mut ClockBuffer) -> Option<Self::Item> {
            match self {
                #(#operate_next_fields, )*
            }
        }
    );

    let operate_get_schema_fields = data.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        quote!(
            Self::#variant_name(e) => e.get_schema()
        )
    });
    let operate_get_schema = quote!(
        fn get_schema(&self) -> Schema {
            match self {
                #(#operate_get_schema_fields, )*
            }
        }
    );

    let gen = quote!(
        impl Operate for #name {
            type Item = Tuple;

            #operate_next

            #operate_get_schema
        }
    );
    gen.into()
}

#[proc_macro_derive(Operate)]
pub fn operate_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_operate_macro(&ast)
}