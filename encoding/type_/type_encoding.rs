/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


pub mod concept {
    use struct_deser::SerializedByteLen;
    use struct_deser_derive::StructDeser;

    use crate::{EncodingSection, WritableKeyFixed};
    use crate::prefix::PrefixID;

    const TYPE_ID_SIZE: usize = 2;

    #[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeIID {
        prefix: PrefixID,
        id: TypeID,
    }

    impl TypeIID {
        pub fn new(prefix: PrefixID, type_id: TypeID) -> TypeIID {
            TypeIID { prefix: prefix, id: type_id }
        }

        pub fn prefix(&self) -> PrefixID {
            self.prefix
        }
    }

    impl WritableKeyFixed for TypeIID {
        fn key_section_id(&self) -> u8 {
            EncodingSection::Schema.id()
        }
    }

    #[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeID {
        bytes: [u8; TYPE_ID_SIZE],
    }

    impl TypeID {
        pub fn new(id: [u8; TYPE_ID_SIZE]) -> TypeID {
            TypeID { bytes: id }
        }

        pub(crate) const fn size() -> usize {
            TypeID::BYTE_LEN
        }
    }

    pub mod root {
        use crate::label::Label;

        pub enum Root {
            Entity,
            Attribute,
            Relation,
            Role,
        }

        impl Root {
            // TODO this should be CONST
            pub fn label(&self) -> Label {
                match self {
                    Root::Entity => Label { name: String::from("entity"), scope: None },
                    Root::Attribute => Label { name: String::from("attribute"), scope: None },
                    Root::Relation => Label { name: String::from("relation"), scope: None },
                    Root::Role => Label { name: String::from("role"), scope: None },
                }
            }
        }
    }
}

pub mod index {
    use struct_deser::SerializedByteLen;
    use struct_deser_derive::StructDeser;

    use crate::{EncodingSection, Serialisable, WritableKeyDynamic, WritableKeyFixed};
    use crate::prefix::{Prefix, PrefixID};
    use crate::type_::type_encoding::concept::TypeIID;
    use crate::value::StringBytes;

    #[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeIIDLabelIndex {
        prefix: PrefixID,
        iid: TypeIID,
    }

    impl TypeIIDLabelIndex {
        pub fn new(iid: TypeIID, label: &str) -> (TypeIIDLabelIndex, StringBytes) {
            (
                TypeIIDLabelIndex {
                    prefix: Prefix::TypeLabelIndex.as_id(),
                    iid: iid,
                },
                StringBytes::encode(label)
            )
        }

        pub(crate) const fn size() -> usize {
            TypeIIDLabelIndex::BYTE_LEN
        }
    }

    impl WritableKeyFixed for TypeIIDLabelIndex {
        fn key_section_id(&self) -> u8 {
            EncodingSection::Schema.id()
        }
    }

    pub struct LabelTypeIIDIndex {
        prefix: PrefixID,
        label: StringBytes,
    }

    impl LabelTypeIIDIndex {
        pub fn new(label: &str) -> LabelTypeIIDIndex {
            LabelTypeIIDIndex {
                prefix: Prefix::LabelTypeIndex.as_id(),
                label: StringBytes::encode(label),
            }
        }
    }

    impl Serialisable for LabelTypeIIDIndex {
        fn serialised_size(&self) -> usize {
            self.prefix.serialised_size() + self.label.serialised_size()
        }

        fn serialise_into(&self, array: &mut [u8]) {
            debug_assert_eq!(array.len(), self.serialised_size());
            let slice = &mut array[0..self.prefix.serialised_size()];
            self.prefix.serialise_into(slice);
            let slice = &mut array[self.prefix.serialised_size()..self.serialised_size()];
            self.label.serialise_into(slice)
        }
    }

    impl WritableKeyDynamic for LabelTypeIIDIndex {
        fn key_section_id(&self) -> u8 {
            EncodingSection::Schema.id()
        }
    }
}

mod connection {}

