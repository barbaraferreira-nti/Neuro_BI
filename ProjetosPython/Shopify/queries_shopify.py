class queries:
    @staticmethod
    def query_orders(dataI, dataF):
        query_orders = f"""
        {{
        orders(
            first: 200,
            query: "created_at:>={dataI} created_at:<={dataF}",
            reverse: true,
            sortKey: CREATED_AT
        ) {{
            edges {{
            node {{
                id
                createdAt
                name
                updatedAt
                processedAt
                cancelledAt

                lineItems(first: 10) {{
                edges {{
                    node {{
                    id
                    product {{
                        id
                    }}
                    quantity
                    discountedUnitPriceAfterAllDiscountsSet {{
                        shopMoney {{
                            amount
                        }}
                    }}
                    discountedTotal
                    discountAllocations {{
                        allocatedAmount {{
                            amount
                        }}
                    }}
                    }}
                }}
                }}

                customer {{
                id
                displayName
                email
                phone
                }}

                localizedFields(first: 10) {{
                edges {{
                    node {{
                    key
                    title
                    value
                    }}
                }}
                }}

                displayFinancialStatus
                discountCode
                discountCodes
                totalPrice
                totalReceived
                totalDiscounts
                currentSubtotalLineItemsQuantity
                paymentGatewayNames
                discountApplications(first: 10) {{
                    nodes {{
                        allocationMethod
                        targetSelection
                        targetType
                        value {{
                            ... on PricingPercentageValue {{
                                __typename
                                percentage
                            }}
                        }}
                    }}
                }}

                shippingAddress {{
                address1
                address2
                city
                province
                provinceCode
                zip
                }}

                shippingLine {{
                title
                price
                }}

                referrerDisplayText
            }}
            }}
        }}
        }}
        """

        return query_orders

    query_customers = """


    """

    @staticmethod
    def query_products():
        query_products = """
        {
        products(first: 100) {
            edges {
            node {
                id
                title
                createdAt
                publishedAt
                updatedAt
                status
                productType
                category {
                id
                name
                }
                storefrontId
            }
            }
        }
        }
        """
        return query_products

    query_teste = """
    FROM orders
    SHOW order_id, customer_id, customer_name, customer_email, product_id,
        product_title, order_fulfillment_status
    SINCE startOfDay(-1d) UNTIL today

    """